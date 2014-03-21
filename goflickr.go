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
	"sync"
	"time"
)

const (
	APP_KEY              = "c021a5195dabedc5e9f4c451cf2a6f70"
	APP_SECRET           = "ddf3cfe6d6403464"
	APP_WEB              = "http://127.0.0.1:8088/flickr/"
	OAUTH_FILE           = ".flickr_oauth"
	FAILED_FILES_FILE    = ".flickr_failed"
	QUEUE_SIZE           = 10
	CHECK_TICKETS_PERIOD = 1

	OPERATION_UPLOAD           = 0
	OPERATION_UPDATE_TIMESTAMP = 1
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
	ID                string
	photos            map[string][]*FlickrPhoto // We get arrays since multiple photos can have the same name (because no restriction on subdirectories)
	path_failed_files string
	failed_files      FlickrFailedFiles
}

func NewFlickrPhotoSet() (fps *FlickrPhotoSet) {
	fps = new(FlickrPhotoSet)
	fps.photos = make(map[string][]*FlickrPhoto)
	return fps
}

func (fps *FlickrPhotoSet) InitializePhotos(fc *FlickrClient) {
	g_lwt_regex := regexp.MustCompile("vision:lwt=([0-9]+)")

	photoset_resp := fc.Call("photosets.getPhotos", CALL_GET, "photoset_id", fps.ID, "extras", "tags")
	for _, photo_val := range photoset_resp.GetPath("photoset", "photo").MustArray() {
		fp := new(FlickrPhoto)
		photo_name := fetch_val(photo_val, "title")
		fp.ID = fetch_val(photo_val, "id")
		photo_tags := fetch_val(photo_val, "tags")
		lwt_string := g_lwt_regex.FindStringSubmatch(photo_tags)
		if lwt_string != nil {
			var timestamp, _ = strconv.Atoi(lwt_string[1])
			fp.TimeStamp = int64(timestamp)
		} else {
			log.Printf("couldn't find  %s in %s", g_lwt_regex.String(), photo_tags)
		}
		fps.photos[photo_name] = append(fps.photos[photo_name], fp)
	}
}

func (fps *FlickrPhotoSet) load_failed_files(dir_name string) {
	var path_failed_files = filepath.Join(dir_name, FAILED_FILES_FILE)
	fps.path_failed_files = path_failed_files

	if file, err := os.Open(path_failed_files); err == nil {
		defer file.Close()
		if stats, err := file.Stat(); err == nil {
			var buffer = make([]byte, stats.Size())
			file.Read(buffer)
			json.Unmarshal(buffer, &fps.failed_files)
			for _, failed_file := range fps.failed_files.Files {
				var flickr_photo = new(FlickrPhoto)
				flickr_photo.ID = "-2"
				flickr_photo.TimeStamp = failed_file.Date
				var photo_name = get_file_no_ext(failed_file.Path)
				fps.photos[photo_name] = append(fps.photos[photo_name], flickr_photo)
			}
		}
	}
}

func (fps *FlickrPhotoSet) save_failed_files() {
	if len(fps.failed_files.Files) > 0 {
		if file, err := os.OpenFile(fps.path_failed_files, os.O_WRONLY|os.O_CREATE, 0); err == nil {
			defer file.Close()
			if buffer, err := json.Marshal(fps.failed_files); err == nil {
				file.Write(buffer)
			}

		}
	}
}

func (fps *FlickrPhotoSet) add_failed_file(filename string, timestamp int64) {
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
	time_allowed  int

	AddPhotoQueue chan *FlickrAddPhotoData
	DoneAddPhoto  chan bool
	WGAddPhoto    sync.WaitGroup

	PhotosToAdd map[string]*FlickrAddPhotoData
	Tickets     string

	StartTime       time.Time
	AllowedDuration float64

	update_timestamp int32
	operation_type   uint32
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

func (fb *FlickrBackr) AddPhotoFromTicket(ticket_id string, photo_id string) {
	if _, ok := fb.PhotosToAdd[ticket_id]; ok {
		var photo_data, _ = fb.PhotosToAdd[ticket_id]
		photo_data.Photo.ID = photo_id
		fb.AddPhoto(photo_data)
	}
}

func (fb *FlickrBackr) AddFailedPhoto(ticket_id string) {
	if _, ok := fb.PhotosToAdd[ticket_id]; ok {
		var photo_data, _ = fb.PhotosToAdd[ticket_id]
		var fps = photo_data.UploadData.PhotoSet
		var photo_title = photo_data.UploadData.PhotoName()
		fps.add_failed_file(photo_title, photo_data.Photo.TimeStamp)
	} else {
		panic(fmt.Sprintf("couldn't find photo with ticket =^%v", ticket_id))
	}
}

func (fb *FlickrBackr) Upload(data *FlickrUploadData) {
	var upload_success = false
	var ticket_id = ""

	if fb.DryRun {
		log.Printf("%v (%v) -> %v \n", data.FullPath, data.FileInfo.ModTime().Unix(), data.PhotoSetTitle)
		<-fb.DoneAddPhoto
		return
	}

	for i := 0; i < 5; i++ {
		var response = fb.FC.Upload(data.FullPath, data.PhotoName(),
			"tags", fmt.Sprintf("gobackr vision:lwt=%v", data.FileInfo.ModTime().Unix()),
			"is_public", "0",
			"is_family", "1",
			"is_friend", "1",
			"async", "1")

		if response.Err.Code == 0 {
			upload_success = true
			ticket_id = response.TickedID
			break
		} else if response.Err.Code == 502 { // Server closed connection
			time.Sleep(time.Duration(5) * time.Second)
		} else { // File type unsupported:
			panic(response.Err.Msg)
		}

	}

	if upload_success {
		fb.WGAddPhoto.Add(1)
		var addPhotoData = new(FlickrAddPhotoData)
		addPhotoData.UploadData = data
		addPhotoData.TicketID = ticket_id
		addPhotoData.Photo = new(FlickrPhoto)
		addPhotoData.Photo.TimeStamp = data.FileInfo.ModTime().Unix()

		fb.AddPhotoQueue <- addPhotoData
	} else {
		log.Printf("Upload failed for %v", data.FullPath)
	}

}

type TicketsData struct {
	Complete json.Number `json:"complete"`
	Id       string      `json:"id"`
	PhotoId  string      `json:"photoid"`
}

type TicketsDataArray struct {
	Tickets []TicketsData `json:"ticket"`
}

type CheckTicketsResponse struct {
	Tickets TicketsDataArray `json:"uploader"`
	Stat    string           `json:"stat"`
}

func (fb *FlickrBackr) ProcessCurrentTickets() {
	time.Sleep(time.Duration(CHECK_TICKETS_PERIOD) * time.Second)

	if fb.Tickets == "" {
		return
	}

	var resp CheckTicketsResponse
	fb.FC.CallRest(&resp, "photos.upload.checkTickets", CALL_GET,
		"tickets", fb.Tickets)

	if resp.Stat != "ok" {
		return
	}

	for _, ticket := range resp.Tickets.Tickets {
		var ticket_complete, _ = ticket.Complete.Int64()

		if ticket_complete == 0 {
			continue
		}

		if ticket_complete == 1 {
			fb.AddPhotoFromTicket(ticket.Id, ticket.PhotoId)
		} else if ticket_complete == 2 {
			// Something went wrong with the file, don't process it again on next run
			fb.AddFailedPhoto(ticket.Id)
		}

		fb.Tickets = strings.Replace(fb.Tickets, ticket.Id+",", "", 1)
		delete(fb.PhotosToAdd, ticket.Id)
		<-fb.DoneAddPhoto
		fb.WGAddPhoto.Done()
	}
}

func (fb *FlickrBackr) FlickrCheckTicketsQueue() {
	for {
		select {
		case new_data := <-fb.AddPhotoQueue:
			fb.PhotosToAdd[new_data.TicketID] = new_data
			fb.Tickets = fb.Tickets + fmt.Sprintf("%v,", new_data.TicketID)
		default:
			fb.ProcessCurrentTickets()
		}

	}
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
	fb.time_allowed = time_allowed

	fb.photosets_ids = make(map[string]*FlickrPhotoSet)
	fb.populate_photosets()

	fb.DoneAddPhoto = make(chan bool, QUEUE_SIZE)
	fb.AddPhotoQueue = make(chan *FlickrAddPhotoData, QUEUE_SIZE)
	fb.PhotosToAdd = make(map[string]*FlickrAddPhotoData)

	fb.StartTime = time.Now()
	fb.AllowedDuration = float64(time_allowed)
	fb.operation_type = OPERATION_UPLOAD

	return fb
}

func check_file_extension(filename string) (bool, string) {
	mime_type := mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	return strings.HasPrefix(mime_type, "image/") || strings.HasPrefix(mime_type, "video/"), mime_type
}

func (fps *FlickrPhotoSet) file_exists_on_flickr(target_file os.FileInfo, dry_run bool) bool {
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

func (fb *FlickrBackr) upload_photo_to_flickr(photo_path string, target_file os.FileInfo, fps *FlickrPhotoSet, photoset_name string) bool {
	fb.DoneAddPhoto <- false
	fud := new(FlickrUploadData)
	fud.FullPath = photo_path
	fud.FileInfo = target_file
	fud.PhotoSet = fps
	fud.PhotoSetTitle = photoset_name
	fb.Upload(fud)
	return true
}

type Tag struct {
	ID  string `json:"id"`
	Raw string `json:"raw"`
}

func (fb *FlickrBackr) process_photo(photo_path string, target_file os.FileInfo, fps *FlickrPhotoSet, photoset_name string) bool {
	if time.Now().Sub(fb.StartTime).Minutes() >= fb.AllowedDuration {
		log.Printf("out of time, aborting")
		return false
	}

	if is_supported, _ := check_file_extension(target_file.Name()); is_supported == false {
		return true
	}

	if fps.file_exists_on_flickr(target_file, fb.DryRun) {
		return true
	}

	fb.upload_photo_to_flickr(photo_path, target_file, fps, photoset_name)
	return true
}

func process_directory(fb *FlickrBackr, target_dir string, set_name string, fps *FlickrPhotoSet) bool {

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
			if !process_directory(fb, full_name, set_name, fps) {
				return false
			}
		} else {
			if !fb.process_photo(full_name, dir_res[idx], fps, set_name) {
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

	fps.load_failed_files(dir_name)

	return fps
}

func (fb *FlickrBackr) ReleasePhotoSet(fps *FlickrPhotoSet) {
	fb.WGAddPhoto.Wait()
	fps.save_failed_files()
}

func execute(target_dir string, time_allowed int, dry_run bool, is_sub_dir bool) {

	fb := NewFlickrBackr(time_allowed, dry_run)

	go fb.FlickrCheckTicketsQueue()

	if is_sub_dir {
		var set_name = filepath.Base(target_dir)
		fps := fb.InitPhotoSet(set_name, target_dir)
		process_directory(fb, target_dir, set_name, fps)
		fb.ReleasePhotoSet(fps)
	} else {
		clean_target_dir, _ := filepath.Abs(target_dir)
		f, err := os.Open(clean_target_dir)
		defer f.Close()
		if err != nil {
			panic(err.Error())
		}
		dir_res, _ := f.Readdir(-1)

		for idx := range dir_res {
			full_name := filepath.Join(clean_target_dir, dir_res[idx].Name())

			if dir_res[idx].IsDir() {
				var set_name = filepath.Base(full_name)
				fps := fb.InitPhotoSet(set_name, full_name)
				if !process_directory(fb, full_name, set_name, fps) {
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

	log.SetFlags(0)

	execute(target_dir, time_allowed, *dry_run, *is_sub_dir)

}
