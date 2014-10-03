package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	CNES_BASE_URL = "http://cnes.datasus.gov.br/"
	WORKERS       = 30
)

type NewState struct {
	Name string
	Url  string
}

type NewCity struct {
	Name string
	Url  string
	IBGE string
}

var StateQueue chan NewState
var CityQueue chan NewCity

var CACHE_FOLDER = os.Getenv("CACHE_FOLDER")

func main() {
	StateQueue = make(chan NewState)
	CityQueue = make(chan NewCity)

	StartRequestService(WORKERS)
	Collector()
}

func Collector() {
	doc, err := request(CNES_BASE_URL + "Lista_Tot_Es_Estado.asp")

	if err != nil {
		log.Println("Err", err, "Sleeping...")
		time.Sleep(2 * time.Second)
		Collector()
		return
	}

	var wg sync.WaitGroup

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		go func() {
			Workers()
			wg.Done()
		}()
	}

	getStates(doc)

	wg.Wait()
}

func Workers() {
	for {
		select {
		case state := <-StateQueue:
			doc, err := request(state.Url)
			if err != nil {
				log.Println("ERROR", err)
				time.Sleep(5 * time.Second)
				StateQueue <- state
			} else {
				getCities(doc)
				log.Println("TODO:", "Saving state url on DB", state)
			}
		case city := <-CityQueue:
			doc, err := request(city.Url)
			if err != nil {
				log.Println("ERROR", err)
				time.Sleep(5 * time.Second)
				CityQueue <- city
			} else {
				getEntities(doc)
				log.Println("TODO:", "Saving city url on DB", city)
			}
		}
	}
}

func getStates(doc *goquery.Document) {
	doc.Find("div[style = 'width:300; height:209; POSITION: absolute; TOP: 185px; LEFT: 400px; overflow:auto'] table tr").Each(func(_ int, s *goquery.Selection) {
		data := s.Find("td")
		name := strings.TrimSpace(data.Eq(0).Text())
		urlState, _ := data.Eq(0).Find("a").Attr("href")
		StateQueue <- NewState{Name: name, Url: CNES_BASE_URL + urlState}
	})
}

func getCities(doc *goquery.Document) {
	doc.Find("div[style = 'width:450; height:300; POSITION: absolute; TOP: 201px; LEFT: 180px; overflow:auto'] table tr").Each(func(_ int, s *goquery.Selection) {

		go func(data *goquery.Selection) {
			ibge := data.Eq(0).Text()
			name := data.Eq(1).Text()
			urlCity, _ := data.Eq(1).Find("a").Attr("href")

			CityQueue <- NewCity{Name: strings.TrimSpace(name), Url: CNES_BASE_URL + urlCity, IBGE: ibge}
		}(s.Find("td"))
	})
}

func getEntities(doc *goquery.Document) {
	doc.Find("div[style='width:539; height:500; POSITION: absolute; TOP:198px; LEFT: 121px; overflow:auto'] table a").Each(func(_ int, s *goquery.Selection) {
		log.Println(s.Text())
	})
}

/// Requests

type NewRequest struct {
	Url string
	C   chan Response
}

type Response struct {
	Doc *goquery.Document
	Err error
}

var (
	RequestQueue = make(chan NewRequest)
)

func StartRequestService(n int) {
	client := http.Client{}

	for i := 0; i < n; i++ {
		go processRequestQueue(i, &client)
	}
}

func processRequestQueue(i int, client *http.Client) {
	workerID := strconv.Itoa(i)

	log.Println("#"+workerID, "REQUEST WORKER STARTED")
	for {
		var (
			doc *goquery.Document
			err error
		)
		r := <-RequestQueue
		log.Println("#"+workerID, "GET", r.Url)

		filename := urlToFilename(r.Url)
		log.Println("trying", filename)

		if _, err = os.Stat(filename); err == nil {
			file, err := os.Open(filename)
			if err == nil {
				log.Println("#"+workerID, "FROM CACHE", r.Url)
				doc, err = goquery.NewDocumentFromReader(file)
			}
		} else {
			doc, err = makeRequest(r.Url, client)
		}

		log.Println("#"+workerID, "DONE", r.Url)
		r.C <- Response{
			Doc: doc,
			Err: err,
		}

		<-time.After(4 * time.Second)
	}
}

func makeRequest(urlS string, client *http.Client) (doc *goquery.Document, err error) {
	var (
		req *http.Request
		res *http.Response
	)
	urlP, err := url.Parse(urlS)
	if err != nil {
		log.Fatal(err)
	}
	q := urlP.Query()
	urlP.RawQuery = q.Encode()

	req, err = http.NewRequest("GET", urlP.String(), nil)
	if err != nil {
		log.Println("ERROR", err)
	} else {
		req.Header.Set("User-Agent", "Mozilla/5.0 (DataBr.io Crawler) AppleWebKit/537.13+ (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2")
		res, err = client.Do(req)

		if err != nil {
			log.Println("ERROR", err)
		} else {
			data, _ := ioutil.ReadAll(res.Body)
			ioutil.WriteFile(urlToFilename(urlS), data, os.ModePerm)
			doc, err = goquery.NewDocumentFromReader(res.Body)
			res.Body.Close()
		}
	}

	return doc, err
}

var urlToFileRegex = regexp.MustCompile(`\W`)

func urlToFilename(u string) string {
	return CACHE_FOLDER + "/" + urlToFileRegex.ReplaceAllString(u, "-")
}

var Errors = make(map[string]int64, 0)

func request(u string) (*goquery.Document, error) {
	c := make(chan Response)
	RequestQueue <- NewRequest{Url: u, C: c}
	r := <-c

	if r.Err != nil {
		if Errors[u] > 3 {
			return nil, r.Err
		}
		Errors[u] = Errors[u] + 1
		log.Println("ERR", r.Err, ", counter:", Errors[u])
		time.Sleep(2 * time.Second)
		return request(u)
	} else {
		return r.Doc, r.Err
	}
}
