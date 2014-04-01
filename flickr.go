package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/bitly/go-simplejson"
	"github.com/pthiben/goauth"
)

const (
	FLICKR_OAUTH  = "http://www.flickr.com/services/oauth/"
	FLICKR_REST   = "http://api.flickr.com/services/rest"
	FLICKR_UPLOAD = "http://up.flickr.com/services/upload/"
	CALL_GET      = 0
	CALL_POST     = 1
)

type FlickrClient struct {
	Api_Key    string
	App_Secret string
	App_Web    string
	OAuthPath  string

	oauth *oauth.OAuth
}

func NewFlickrFlient(api_key string, app_secret string, app_web string, oauth_path string) (c *FlickrClient) {
	c = new(FlickrClient)
	c.Api_Key = api_key
	c.App_Secret = app_secret
	c.App_Web = app_web
	c.OAuthPath = oauth_path
	c.get_oauth_from_cache(c.OAuthPath)
	return c
}

type RestResult interface {
}

func (fc *FlickrClient) CallRest(js RestResult, method string, get_or_post int32, params ...string) {

	params_map := make(map[string]string)

	params_map["method"] = "flickr." + method
	params_map["format"] = "json"
	params_map["nojsoncallback"] = "1"

	var key string
	for idx, val := range params {
		if idx%2 == 0 {
			key = val
		} else {
			params_map[key] = val
		}

	}

	var r *http.Response
	var err error

	switch get_or_post {
	case CALL_GET:
		r, err = fc.oauth.Get(FLICKR_REST, params_map)
		break
	case CALL_POST:
		r, err = fc.oauth.Post(FLICKR_REST, params_map)
		break
	default:
		log.Fatal(fmt.Sprintln("goflickr Upload: Unsupported Call method %d", get_or_post))
		break
	}

	if err != nil {
		log.Panic(err.Error())
	}

	if r.StatusCode != 200 {
		log.Fatal(fmt.Sprintln("goflickr CallRest: Error code %d", r.StatusCode))
		log.Panic(fmt.Sprintln("Error code %d", r.StatusCode))
	}

	response, err := ioutil.ReadAll(r.Body)

	if err != nil {
		log.Panic(err.Error())
	}

	err = json.Unmarshal(response, js)
	if err != nil {
		log.Println(err.Error())
		log.Println(bytes.NewBuffer(response).String())
		log.Panic("")
	}
}

func (fc *FlickrClient) Call(method string, get_or_post int32, params ...string) (js *simplejson.Json) {

	params_map := make(map[string]string)

	params_map["method"] = "flickr." + method
	params_map["format"] = "json"
	params_map["nojsoncallback"] = "1"

	var key string
	for idx, val := range params {
		if idx%2 == 0 {
			key = val
		} else {
			params_map[key] = val
		}

	}

	var r *http.Response
	var err error

	switch get_or_post {
	case CALL_GET:
		r, err = fc.oauth.Get(FLICKR_REST, params_map)
		break
	case CALL_POST:
		r, err = fc.oauth.Post(FLICKR_REST, params_map)
		break
	default:
		log.Fatal(fmt.Sprintln("Unsupported Call method %d", get_or_post))
		break
	}

	if err != nil {
		log.Panic(err.Error())
	}

	if r.StatusCode != 200 {
		log.Fatal(fmt.Sprintln("goflickr Call: Error code %d", r.StatusCode))
		log.Panic(fmt.Sprintln("Error code %d", r.StatusCode))
	}

	response, err := ioutil.ReadAll(r.Body)

	if err != nil {
		log.Panic(err.Error())
	}

	js, err = simplejson.NewJson(response)

	if err != nil {
		log.Fatal(err)
		log.Panic(err.Error())
	}

	return js

}

type UploadError struct {
	Code int    `xml:"code,attr"`
	Msg  string `xml:"msg,attr"`
}

type UploadResponse struct {
	XMLName  xml.Name    `xml:"rsp"`
	Stat     string      `xml:"stat,attr"`
	Err      UploadError `xml:"err"`
	TickedID string      `xml:"ticketid"`
}

// Creates a new file upload http request with optional extra params
func newfileUploadRequest(uri string, params map[string]string, paramName, path string) (*http.Request, error, string) {

	fd, err := os.Open(path)
	if err != nil {
		log.Panic(err.Error())
		return nil, err, ""
	}
	defer fd.Close()

	body := &bytes.Buffer{}
	mpw := multipart.NewWriter(body)

	for k, v := range params {
		if err := mpw.WriteField(k, v); err != nil {
			return nil, err, ""
		}
	}

	fw, err := mpw.CreateFormFile(paramName, filepath.Base(path))
	if err != nil {
		log.Panic(err.Error())
		return nil, err, ""
	}

	_, err = io.Copy(fw, fd)

	if err == io.ErrUnexpectedEOF {
		log.Panic(err.Error())
	}

	if err != nil {
		log.Panic(err.Error())
	}

	err = mpw.Close()
	if err != nil {
		log.Panic(err.Error())
	}

	req, err := http.NewRequest("POST", uri, body)

	if err != nil {
		log.Panic(err.Error())
	}

	// Don't forget to set the content type, this will contain the boundary.
	req.Header.Set("Content-Type", mpw.FormDataContentType())

	return req, err, mpw.Boundary()
}

func post_data(fc *FlickrClient, url_ string, params map[string]string, filename string) (r *http.Response, err error) {
	req, err, _ := newfileUploadRequest(url_, params, "photo", filename)

	if err != nil {
		log.Panic(err.Error())
		return nil, err
	}

	var oauth_gen_params = params

	var _, oauth_params = fc.oauth.MakeParams(url_, oauth_gen_params)

	req.Close = true
	req.Header.Set("Authorization", "OAuth ")

	req.TransferEncoding = []string{"chunked"}

	first := true
	for k, v := range oauth_params {
		if first {
			first = false
		} else {
			req.Header["Authorization"][0] += ",\n    "
		}
		req.Header["Authorization"][0] += k + "=\"" + v + "\""
	}

	req.URL, err = url.Parse(url_)
	if err != nil {
		return nil, err
	}

	resp, err := oauth.Send(req)

	if err != nil {
		var err_msg = fmt.Sprintf("oauth.Send failed for %v bytes\n", req.ContentLength)
		err_msg += fmt.Sprintf("Header: \n%v\n", req.Header)
		err_msg += fmt.Sprintf("Body: \n%v\n", to_string(req.Body))
		log.Panicf(err_msg)
	}

	return resp, err
}

func to_string(ior io.ReadCloser) string {
	buf := new(bytes.Buffer)
	buf.ReadFrom(ior)
	return buf.String()
}

func (fc *FlickrClient) Upload(filename string, title string, params ...string) UploadResponse {

	params_map := make(map[string]string)

	params_map["title"] = title

	v := UploadResponse{Err: UploadError{Code: -1, Msg: "Response failed"}}

	var key string
	for idx, val := range params {
		if idx%2 == 0 {
			key = val
		} else {
			params_map[key] = val
		}

	}

	r, err := post_data(fc, FLICKR_UPLOAD, params_map, filename)

	if err != nil {
		log.Panic(err.Error())
		return v
	}

	if r.StatusCode != 200 {
		log.Println(fmt.Errorf("post_data failed: %v\n", to_string(r.Body)))
		v.Err.Code = r.StatusCode
		return v
	}

	response, err := ioutil.ReadAll(r.Body)
	// str_resp := bytes.NewBuffer(response).String()
	// fmt.Println(str_resp)

	if err != nil {
		log.Panic(err.Error())
	}

	err = xml.Unmarshal(response, &v)

	if err != nil {
		log.Panic(err.Error())
	}

	if v.Stat == "ok" {
		v.Err.Code = 0
		v.Err.Msg = ""
	}

	return v
}

func (fc *FlickrClient) get_oauth_from_cache(path string) {

	fc.oauth = load_oauth_from_cache(path)
	if !fc.validate_connectivity(fc.oauth) {
		fc.oauth = create_oauth(path)
	}

	if fc.oauth == nil {
		log.Panic("")
	}

}

func init_oauth(o *oauth.OAuth) {
	o.ConsumerKey = APP_KEY
	o.ConsumerSecret = APP_SECRET
	o.Callback = APP_WEB

	o.RequestTokenURL = FLICKR_OAUTH + "request_token"
	o.OwnerAuthURL = FLICKR_OAUTH + "authorize"
	o.AccessTokenURL = FLICKR_OAUTH + "access_token"

	o.SignatureMethod = "HMAC-SHA1"

}

func create_oauth(path string) *oauth.OAuth {
	o := new(oauth.OAuth)
	init_oauth(o)

	channel := make(chan string)

	go func() {

		http.HandleFunc("/flickr/", func(w http.ResponseWriter, r *http.Request) {
			channel <- r.FormValue("oauth_verifier")
		})

		http.ListenAndServe(":8088", nil)

	}()

	err := o.GetRequestToken()
	if err != nil {
		fmt.Println("GetRequestToken " + string(err.Error()))
		return nil
	}

	url, err := o.AuthorizationURL()
	if err != nil {
		fmt.Println("AuthorizationURL " + string(err.Error()))
		return nil
	}

	launch_browser_cmd := exec.Command("cmd", "/c", "start", url)

	err = launch_browser_cmd.Start()

	if err != nil {
		log.Panic(err)
	}

	err = launch_browser_cmd.Wait()

	verification_string := <-channel

	err = o.GetAccessToken(verification_string)
	if err != nil {
		fmt.Println("GetAccessToken " + string(err.Error()))
		return nil
	}

	save_oauth_to_cache(o, path)

	return o
}

func (fc *FlickrClient) validate_connectivity(o *oauth.OAuth) bool {

	if o == nil {
		return false
	}

	js := fc.Call("test.login", CALL_GET)
	resp_stat, err := js.GetPath("stat").String()

	if err != nil {
		log.Panic(err)
		return false
	}

	if resp_stat != "ok" {
		return false
	}

	return true
}

func load_oauth_from_cache(path string) (out_auth *oauth.OAuth) {
	// open input file

	_, err := os.Stat(path)

	if err != nil && os.IsNotExist(err) {
		fmt.Println(path + " not found")
		return nil
	}

	fi, err := os.Open(path)
	if err != nil {
		log.Panic(err)
	}
	// close fi on exit and check for its returned error
	defer func() {
		if err := fi.Close(); err != nil {
			log.Panic(err)
		}
	}()

	fi_stat, _ := fi.Stat()

	data := make([]byte, fi_stat.Size())

	_, err = fi.Read(data)
	if err != nil {
		log.Panic(err)
	}

	o := new(oauth.OAuth)
	if err := json.Unmarshal(data, &o); err != nil {
		log.Panic(err)
	}

	return o
}

func save_oauth_to_cache(o *oauth.OAuth, path string) {
	b, err := json.Marshal(o)

	if err != nil {
		log.Panic(err)
	}

	fo, err := os.Create(path)
	if err != nil {
		log.Panic(err)
	}

	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			log.Panic(err)
		}
	}()

	fo.Write(b)
}
