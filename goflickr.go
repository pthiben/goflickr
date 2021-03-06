package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	APP_KEY                   = "c021a5195dabedc5e9f4c451cf2a6f70"
	APP_SECRET                = "ddf3cfe6d6403464"
	APP_WEB                   = "http://127.0.0.1:8088/flickr/"
	OAUTH_FILE                = ".flickr_oauth"
	FAILED_FILES_FILE         = ".flickr_failed"
	MAX_FILES_IN_UPLOAD_QUEUE = 10
)

type FlickrPhoto struct {
	ID        string
	TimeStamp int64
}

type FlickrFailedFile struct {
	Path string
	Date int64
}

type FlickrFailedFiles struct {
	Files []FlickrFailedFile
}

type FlickrPhotoSet struct {
	ID                string                    // Flickr ID for the photoset
	photos            map[string][]*FlickrPhoto // We get arrays since multiple photos can have the same name (because no restriction on subdirectories)
	path_failed_files string                    // path of the DB containing files that failed in a previous backup
	failed_files      FlickrFailedFiles         // List of the files that are failing in this execution
}

func NewFlickrPhotoSet() (fps *FlickrPhotoSet) {
	fps = new(FlickrPhotoSet)
	fps.photos = make(map[string][]*FlickrPhoto)
	return fps
}

func (fps *FlickrPhotoSet) InitializePhotos(fc *FlickrClient) {
	g_lwt_regex := regexp.MustCompile("vision:lwt=([0-9]+)")
	g_lwt_regex2 := regexp.MustCompile("{vision}:{lwt}=([0-9]+)")

	var photoset_info = fc.Call("photosets.getInfo", CALL_GET, "photoset_id", fps.ID)

	var photo_count = photoset_info.GetPath("photoset", "photos").MustInt(1)

	const photos_per_page = 500

	var page_count = (photo_count-1)/photos_per_page + 1

	for page_idx := 0; page_idx < page_count; page_idx++ {

		photoset_resp := fc.Call("photosets.getPhotos", CALL_GET, "photoset_id", fps.ID, "extras", "tags",
			"per_page", fmt.Sprint(photos_per_page), "page", fmt.Sprint(page_idx+1))

		for _, photo_val := range photoset_resp.GetPath("photoset", "photo").MustArray() {
			fp := new(FlickrPhoto)

			photo_name := fetch_val(photo_val, "title")
			fp.ID = fetch_val(photo_val, "id")
			photo_tags := fetch_val(photo_val, "tags")

			lwt_string := g_lwt_regex.FindStringSubmatch(photo_tags)
			lwt_string2 := g_lwt_regex2.FindStringSubmatch(photo_tags)

			if lwt_string != nil {
				var timestamp, _ = strconv.Atoi(lwt_string[1])
				fp.TimeStamp = int64(timestamp)
			} else if lwt_string2 != nil {
				var timestamp, _ = strconv.Atoi(lwt_string2[1])
				fp.TimeStamp = int64(timestamp)
			} else {
				log.Printf("couldn't find  %s in %s", g_lwt_regex.String(), photo_tags)
			}

			fps.photos[photo_name] = append(fps.photos[photo_name], fp)
		}

	}

}

func (fps *FlickrPhotoSet) LoadFailedFiles(dir_name string) {
	var path_failed_files = filepath.Join(dir_name, FAILED_FILES_FILE)
	fps.path_failed_files = path_failed_files

	if file, err := os.Open(path_failed_files); err == nil {
		defer file.Close()
		if stats, err := file.Stat(); err == nil {
			var buffer = make([]byte, stats.Size())
			file.Read(buffer)
			json.Unmarshal(buffer, &fps.failed_files)
			for _, failed_file := range fps.failed_files.Files {
				var flickr_photo = new(FlickrPhoto )
				flickr_photo.ID = "-2"
				flickr_photo.TimeStamp = failed_file.Date
				var photo_name = get_file_no_ext(failed_file.Path)
				fps.photos[photo_name] = append(fps.photos[photo_name], flickr_photo)
			}
		}
	}
}

func (fps *FlickrPhotoSet) SaveFailedFiles() {
	if len(fps.failed_files.Files) > 0 {
		if file, err := os.OpenFile(fps.path_failed_files, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0); err == nil {
			defer file.Close()
			if buffer, err := json.Marshal(fps.failed_files); err == nil {
				file.Write(buffer)
			}

		}
	}
}

func (fps *FlickrPhotoSet) AddFailedFile(filename string, timestamp int64) {
	log.Printf("Adding %v as failed\n", filename)
	var failed_file = FlickrFailedFile{filename, timestamp}
	fps.failed_files.Files = append(fps.failed_files.Files, failed_file)
}

type FlickrUploadData struct {
	FullPath      string
	FileInfo      os.FileInfo
	PhotoSet      *FlickrPhotoSet
	PhotoSetTitle string
}

func get_file_no_ext(path string) string {
	var base_path = filepath.Base(path)
	var extension = filepath.Ext(base_path)
	return path[0 : len(base_path)-len(extension)]
}

func (fud *FlickrUploadData) PhotoName() string {
	return get_file_no_ext(fud.FileInfo.Name())
}

type FlickrAddPhotoData struct {
	UploadData *FlickrUploadData
	Photo      *FlickrPhoto
	TicketID   string
}

type FlickrBackr struct {
	FC            *FlickrClient
	DryRun        bool
	photosets_ids map[string]*FlickrPhotoSet

	PhotosToAdd map[string]*FlickrAddPhotoData
	Tickets     string

	StartTime       time.Time
	AllowedDuration float64

	FilesInUploadQueue int
}

func (fb *FlickrBackr) AddPhoto(data *FlickrAddPhotoData) {
	var fps = data.UploadData.PhotoSet
	var fps_title = data.UploadData.PhotoSetTitle
	// If no photo in photoset, assume this photoset didn't exist
	if len(fps.photos) == 0 {
		var response = fb.FC.Call("photosets.create", CALL_POST, "title", fps_title, "primary_photo_id", data.Photo.ID)
		fps.ID = response.GetPath("photoset", "id").MustString()

	} else {
		fb.FC.Call("photosets.addPhoto", CALL_POST, "photoset_id", fps.ID, "photo_id", data.Photo.ID)
	}

	log.Printf("%v -> %v", data.UploadData.FullPath, fps_title)
	var photo_title = data.UploadData.PhotoName()
	fps.photos[photo_title] = append(fps.photos[photo_title], data.Photo)
}

func (fb *FlickrBackr) OnPhotoUploaded(ticket_id string, photo_id string) {
	if _, ok := fb.PhotosToAdd[ticket_id]; ok {
		var photo_data, _ = fb.PhotosToAdd[ticket_id]
		photo_data.Photo.ID = photo_id
		fb.AddPhoto(photo_data)
	}
}

func (fb *FlickrBackr) OnPhotoFailed(ticket_id string) {
	if _, ok := fb.PhotosToAdd[ticket_id]; ok {
		var photo_data, _ = fb.PhotosToAdd[ticket_id]
		var fps = photo_data.UploadData.PhotoSet
		var photo_title = photo_data.UploadData.PhotoName()
		fps.AddFailedFile(photo_title, photo_data.Photo.TimeStamp)
	} else {
		log.Panic(fmt.Sprintf("couldn't find photo with ticket =^%v", ticket_id))
	}
}

func (fb *FlickrBackr) Upload(data *FlickrUploadData) {
	var upload_success = false
	var ticket_id = ""

	if fb.DryRun {
		log.Printf("%v (%v) -> %v \n", data.FullPath, data.FileInfo.ModTime().Unix(), data.PhotoSetTitle)
		return
	}

	var lastError = 0

	for i := 0; i < 5; i++ {
		var response = fb.FC.Upload(data.FullPath, data.PhotoName(),
			"tags", fmt.Sprintf("gobackr vision:lwt=%v", data.FileInfo.ModTime().Unix()),
			"is_public", "0",
			"is_family", "1",
			"is_friend", "1",
			"async", "1")

		lastError = response.Err.Code

		if response.Err.Code == 0 {
			upload_success = true
			ticket_id = response.TickedID
			break
		} else if response.Err.Code == 502 { // Server closed connection
			time.Sleep(time.Duration(5) * time.Second)
		} else if response.Err.Code == 3 { // Possibly corrupted file
			break
		} else { // File type unsupported:
			log.Fatalf("goflickr Upload failed: (error code %v) : %v", response.Err.Code, response.Err.Msg)
		}

	}

	if upload_success {
		var addPhotoData = new(FlickrAddPhotoData)
		addPhotoData.UploadData = data
		addPhotoData.TicketID = ticket_id
		addPhotoData.Photo = new(FlickrPhoto)
		addPhotoData.Photo.TimeStamp = data.FileInfo.ModTime().Unix()
		fb.PhotosToAdd[ticket_id] = addPhotoData
		fb.Tickets = fb.Tickets + fmt.Sprintf("%v,", ticket_id)
		fb.FilesInUploadQueue++
	} else {
		log.Printf("Upload error %v: ", lastError)
		data.PhotoSet.AddFailedFile(data.PhotoName(), data.FileInfo.ModTime().Unix())
	}

}

type CheckTicketsResponse struct {
	Tickets struct {
		Tickets []struct {
			Complete json.Number `json:"complete"`
			Id       string      `json:"id"`
			PhotoId  string      `json:"photoid"`
		} `json:"ticket"`
	} `json:"uploader"`
	Stat string `json:"stat"`
}

// Returns if there are remaining tickets to process
func (fb *FlickrBackr) ProcessCurrentTickets() bool {

	if fb.Tickets == "" {
		return false
	}

	var resp CheckTicketsResponse
	fb.FC.CallRest(&resp, "photos.upload.checkTickets", CALL_GET,
		"tickets", fb.Tickets)

	if resp.Stat != "ok" {
		return false
	}

	for _, ticket := range resp.Tickets.Tickets {
		var ticket_complete, _ = ticket.Complete.Int64()

		if ticket_complete == 0 {
			continue
		}

		fb.FilesInUploadQueue--

		if ticket_complete == 1 {
			fb.OnPhotoUploaded(ticket.Id, ticket.PhotoId)
		} else if ticket_complete == 2 {
			// Something went wrong with the file, don't process it again on next run
			log.Printf("Error %v for ticket: ", ticket_complete)
			fb.OnPhotoFailed(ticket.Id)
		}

		fb.Tickets = strings.Replace(fb.Tickets, ticket.Id+",", "", 1)
		delete(fb.PhotosToAdd, ticket.Id)
	}

	return fb.Tickets != ""
}

func fetch_val(json_val interface{}, params ...string) string {

	json_val_map := json_val.(map[string]interface{})
	for i := 0; i < len(params)-1; i++ {
		json_val_map = json_val_map[params[i]].(map[string]interface{})
	}

	return json_val_map[params[len(params)-1]].(string)
}

func (fb *FlickrBackr) populate_photosets() {
	resp := fb.FC.Call("photosets.getList", CALL_GET)

	photosets := resp.GetPath("photosets", "photoset").MustArray()
	for _, photoset_val := range photosets {
		id_string := fetch_val(photoset_val, "id")
		title_map := fetch_val(photoset_val, "title", "_content")
		fps := NewFlickrPhotoSet()
		fb.photosets_ids[title_map] = fps
		fps.ID = id_string
	}
}

func NewFlickrBackr(time_allowed int, dry_run bool) (fb *FlickrBackr) {
	fb = new(FlickrBackr)
	fb.FC = NewFlickrFlient(APP_KEY, APP_SECRET, APP_WEB, OAUTH_FILE)

	if fb.FC == nil {
		panic("Can't create FlickrClient")
	}

	fb.DryRun = dry_run

	fb.photosets_ids = make(map[string]*FlickrPhotoSet)
	fb.populate_photosets()

	fb.PhotosToAdd = make(map[string]*FlickrAddPhotoData)

	fb.StartTime = time.Now()
	fb.AllowedDuration = float64(time_allowed)

	return fb
}

func CheckFileExtension(filename string) (bool, string) {
	mime_type := mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	return strings.HasPrefix(mime_type, "image/") || strings.HasPrefix(mime_type, "video/"), mime_type
}

func (fps *FlickrPhotoSet) FileExistsOnFlickr(target_file os.FileInfo, dry_run bool) bool {
	photo_name := get_file_no_ext(target_file.Name())

	var dates = "("
	for _, val := range fps.photos[photo_name] {
		if target_file.ModTime().Unix() == val.TimeStamp {
			return true
		}
		dates += fmt.Sprintf("%d,", val.TimeStamp)
	}

	dates += ")"

	if dry_run {
		log.Printf("missed %v in %v", target_file.ModTime().Unix(), dates)
	}

	return false
}

func (fb *FlickrBackr) UploadPhotoToFlickr(photo_path string, target_file os.FileInfo, fps *FlickrPhotoSet, photoset_name string) bool {

	fud := FlickrUploadData{
		FullPath:      photo_path,
		FileInfo:      target_file,
		PhotoSet:      fps,
		PhotoSetTitle: photoset_name,
	}

	fb.Upload(&fud)
	return true
}

func (fb *FlickrBackr) ProcessPhoto(photo_path string, target_file os.FileInfo, fps *FlickrPhotoSet, photoset_name string) bool {

	for {

		if fb.ProcessCurrentTickets() == false {
			break
		}

		if fb.FilesInUploadQueue < MAX_FILES_IN_UPLOAD_QUEUE {
			break
		}
	}

	if time.Now().Sub(fb.StartTime).Minutes() >= fb.AllowedDuration {
		log.Printf("out of time, aborting")
		return false
	}

	if is_supported, _ := CheckFileExtension(target_file.Name()); is_supported == false {
		return true
	}

	if fps.FileExistsOnFlickr(target_file, fb.DryRun) {
		return true
	}

	fb.UploadPhotoToFlickr(photo_path, target_file, fps, photoset_name)
	return true
}

func ProcessDirectory(fb *FlickrBackr, target_dir string, set_name string, fps *FlickrPhotoSet) bool {

	log.Printf("%s", target_dir)

	clean_target_dir, _ := filepath.Abs(target_dir)

	f, err := os.Open(clean_target_dir)

	if err != nil {
		// Directory might have been deleted, abort, but keep trying other directories
		log.Printf("error %s", err.Error())
		return true
	}

	defer f.Close()

	dir_res, _ := f.Readdir(-1)

	for idx := range dir_res {
		full_name := filepath.Join(clean_target_dir, dir_res[idx].Name())
		if dir_res[idx].IsDir() {
			if !ProcessDirectory(fb, full_name, set_name, fps) {
				return false
			}
		} else {
			if !fb.ProcessPhoto(full_name, dir_res[idx], fps, set_name) {
				return false
			}
		}
	}

	return true
}

func (fb *FlickrBackr) InitPhotoSet(set_name string, dir_name string) *FlickrPhotoSet {
	var fps = fb.photosets_ids[set_name]

	if fps == nil {
		fps = NewFlickrPhotoSet()
		fb.photosets_ids[set_name] = fps
	} else {
		fps.InitializePhotos(fb.FC)
	}

	fps.LoadFailedFiles(dir_name)

	return fps
}

func (fb *FlickrBackr) WaitForUploadingPhotos() {

	for {

		if fb.ProcessCurrentTickets() == false {
			break
		}

		time.Sleep(time.Duration(1) * time.Second)
	}
}

func (fb *FlickrBackr) ReleasePhotoSet(fps *FlickrPhotoSet) {
	fb.WaitForUploadingPhotos()
	fps.SaveFailedFiles()
}

func execute(target_dir string, time_allowed int, dry_run bool, is_sub_dir bool) {

	fb := NewFlickrBackr(time_allowed, dry_run)

	if is_sub_dir {
		var set_name = filepath.Base(target_dir)
		fps := fb.InitPhotoSet(set_name, target_dir)
		ProcessDirectory(fb, target_dir, set_name, fps)
		fb.ReleasePhotoSet(fps)
	} else {
		clean_target_dir, _ := filepath.Abs(target_dir)
		f, err := os.Open(clean_target_dir)
		if err != nil {
			panic(err.Error())
		}
		dir_res, _ := f.Readdir(-1)
		f.Close()

		for idx := range dir_res {
			full_name := filepath.Join(clean_target_dir, dir_res[idx].Name())

			if dir_res[idx].IsDir() {
				var set_name = filepath.Base(full_name)
				fps := fb.InitPhotoSet(set_name, full_name)
				if !ProcessDirectory(fb, full_name, set_name, fps) {
					fb.ReleasePhotoSet(fps)
					break
				}
				fb.ReleasePhotoSet(fps)
			}
		}
	}
}

func main() {

	var target_dir string
	var time_allowed int
	flag.StringVar(&target_dir, "directory", ".", "Directory to sync")
	flag.StringVar(&target_dir, "d", ".", "Directory to sync")
	flag.IntVar(&time_allowed, "t", 1, "Time in mn allowed to complete")
	dry_run := flag.Bool("x", false, "Dry run (do not sync anything)")
	is_sub_dir := flag.Bool("s", false, "Process only sub directory")

	flag.Parse()

	var f, err = os.OpenFile("goflickr.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	execute(target_dir, time_allowed, *dry_run, *is_sub_dir)

}
