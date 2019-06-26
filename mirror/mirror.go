/*
Copyright 2014 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/nf/dl/fetch"
)

var (
	refresh    = flag.Duration("refresh", time.Minute, "refresh interval")
	hideAfter  = flag.Duration("hide", 7*24*time.Hour, "hide done files after this time")
	httpAddr   = flag.String("http", "localhost:8080", "HTTP listen address")
	cacheFile  = flag.String("cache", "cache.json", "state cache file")
	configFile = flag.String("config", "", "config file name")
)

type config struct {
	// BaseURL from which to scrape file names.
	BaseURL string

	// Anchor tag goquery selector.
	Selector string

	// HTTP Basic authentication.
	Username string
	Password string

	// Hour at which to start/stop downloads.
	Start, Stop *int
}

func main() {
	flag.Parse()

	if *configFile == "" {
		log.Fatal("must supply -config")
	}
	cfgB, err := ioutil.ReadFile(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	var cfg config
	if err := json.Unmarshal(cfgB, &cfg); err != nil {
		log.Fatalf("error reading config file: %v", err)
	}
	baseURL = cfg.BaseURL // TODO: ew, gross.

	start, stop := -1, -1
	if cfg.Start != nil {
		start = *cfg.Start
	}
	if cfg.Stop != nil {
		stop = *cfg.Stop
	}

	m := NewManager(
		cfg.BaseURL,
		*refresh,
		start, stop,
		cfg.Selector,
		*cacheFile,
		func(r *http.Request) {
			if cfg.Username != "" || cfg.Password != "" {
				r.SetBasicAuth(cfg.Username, cfg.Password)
			}
		},
	)
	go m.Run()
	http.Handle("/", m)

	// Turn off HTTP/2 in Go 1.6+.
	http.DefaultTransport.(*http.Transport).TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)

	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}

type File struct {
	URL            string
	Received, Size int64
	State          State
	DoneAt         time.Time
}

var baseURL string // TODO: fix this yuckiness.

func (f *File) Name() string {
	p := strings.TrimPrefix(f.URL, baseURL)
	if s, err := url.QueryUnescape(p); err == nil {
		return s
	}
	return p
}

func (f *File) PercentDone() int {
	if f.Received == 0 {
		return 0
	}
	return int(float64(f.Received) / float64(f.Size) * 100)
}

func (f *File) CanStart() bool {
	return f.State == New || f.State == OnHold || f.State == Error
}

func (f *File) CanQueue() bool {
	return f.State == OnHold || f.State == StartNow
}

func (f *File) CanHold() bool {
	return f.State == New || f.State == StartNow
}

type State int

const (
	New State = iota
	OnHold
	StartNow
	InFlight
	Done
	Error
)

func (s State) String() string {
	return map[State]string{
		New:      "New",
		OnHold:   "Hold",
		StartNow: "Starting",
		InFlight: "Downloading",
		Done:     "Done",
		Error:    "Error",
	}[s]
}

func NewManager(baseURL string, refresh time.Duration, startH, stopH int, selector, cache string, rp fetch.RequestPreparer) *Manager {
	fs := make(map[string]*File)
	if err := readCache(cache, &fs); err != nil {
		log.Fatalf("error reading cache %q: %v", cache, err)
	}
	// Reset files that were InFlight to New;
	// the fetch code will take care of resuming them.
	for _, f := range fs {
		if f.State == InFlight {
			f.State = New
		}
	}
	return &Manager{
		base:     baseURL,
		files:    fs,
		refresh:  refresh,
		startH:   startH,
		stopH:    stopH,
		selector: selector,
		cache:    cache,
		rp:       rp,
		start:    make(chan string),
		hold:     make(chan string),
		queue:    make(chan string),
	}
}

type Manager struct {
	base          string
	refresh       time.Duration
	startH, stopH int
	selector      string
	cache         string
	rp            fetch.RequestPreparer

	filesMu sync.RWMutex
	files   map[string]*File // keyed by remote URL

	start, hold, queue chan string
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		if s := r.FormValue("start"); s != "" {
			m.start <- s
			http.Redirect(w, r, "", http.StatusFound)
			return
		}
		if h := r.FormValue("hold"); h != "" {
			m.hold <- h
			http.Redirect(w, r, "", http.StatusFound)
			return
		}
		if q := r.FormValue("queue"); q != "" {
			m.queue <- q
			http.Redirect(w, r, "", http.StatusFound)
			return
		}
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var fs fileList
	m.filesMu.RLock()
	for _, f := range m.files {
		if f.State == Done && time.Since(f.DoneAt) > *hideAfter {
			continue
		}
		fs = append(fs, f)
	}
	m.filesMu.RUnlock()
	sort.Sort(fs)

	var b bytes.Buffer
	err := uiTemplate.Execute(&b, fs)
	if err != nil {
		log.Println(err)
		return
	}
	b.WriteTo(w)
}

type fileList []*File

func (s fileList) Len() int      { return len(s) }
func (s fileList) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s fileList) Less(i, j int) bool {
	if s[i].State == s[j].State {
		return s[i].URL < s[j].URL
	}
	return s[i].State < s[j].State
}

type status struct {
	url string
	fetch.State
}

func (m *Manager) Run() {
	if changed, err := m.update(); err != nil {
		log.Fatal("update:", err)
	} else if changed {
		m.writeCache()
	}
	status := make(chan status)
	fetching := m.fetchNext(status)

	refresh := time.NewTicker(m.refresh)
	defer refresh.Stop()
	for {
		select {
		case <-refresh.C:
			changed, err := m.update()
			if err != nil {
				log.Println("update:", err)
			}
			switch time.Now().Hour() {
			case m.startH:
				changed = m.changeState(New, StartNow) || changed
			case m.stopH:
				changed = m.changeState(StartNow, New) || changed
			}
			if !fetching {
				fetching = m.fetchNext(status)
				changed = fetching || changed
			}
			if changed {
				m.writeCache()
			}
		case s := <-status:
			m.filesMu.Lock()
			f, ok := m.files[s.url]
			if !ok {
				panic(fmt.Sprintf("got state for unknown url %q", s.url))
			}
			if f.State != InFlight && f.State != Done {
				panic(fmt.Sprintf("unexpected state %v for %v", s, f))
			}
			f.Received = s.Downloaded
			done := false
			switch {
			case s.Error != nil:
				log.Println("fetching:", f.URL, "error:", s.Error)
				f.State = Error
				done = true
			case s.Downloaded == f.Size:
				if f.State != Done {
					f.DoneAt = time.Now()
					f.State = Done
					done = true
				}
			}
			m.filesMu.Unlock()
			if done {
				fetching = m.fetchNext(status)
				m.writeCache()
			}
		case u := <-m.start:
			m.setState(u, StartNow, New, OnHold, Error)
			if !fetching {
				fetching = m.fetchNext(status)
				m.writeCache()
			}
		case u := <-m.hold:
			m.setState(u, OnHold, New, StartNow)
		case u := <-m.queue:
			m.setState(u, New, OnHold, StartNow)
		}
	}
}

func (m *Manager) writeCache() {
	m.filesMu.RLock()
	defer m.filesMu.RUnlock()
	if err := writeCache(m.cache, m.files); err != nil {
		log.Println("error writing cache:", err)
	}
}

func (m *Manager) update() (changed bool, err error) {
	urls, err := m.fileURLs(m.base)
	if err != nil {
		return false, err
	}
	m.filesMu.Lock()
	for _, u := range urls {
		if _, ok := m.files[u]; !ok {
			changed = true
			m.files[u] = &File{URL: u}
		}
	}
	m.filesMu.Unlock()
	return changed, nil
}

func (m *Manager) changeState(from, to State) (changed bool) {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	for _, f := range m.files {
		if f.State == from {
			changed = true
			f.State = to
		}
	}
	return changed
}

func (m *Manager) setState(u string, s State, from ...State) {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	f, ok := m.files[u]
	if !ok {
		log.Printf("setState of unknown url %q", u)
		return
	}
	okFromState := false
	for _, fs := range from {
		if f.State == fs {
			okFromState = true
			break
		}
	}
	if !okFromState {
		log.Printf("setState of %q to %v from invalid state %v; ignoring", u, s, f.State)
		return
	}
	f.State = s
}

func (m *Manager) fetchNext(status chan<- status) bool {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	for _, f := range m.files {
		if f.State == StartNow {
			log.Println("fetching:", f.URL)
			m.filesMu.Unlock()
			size, err := m.fetch(f.URL, status)
			m.filesMu.Lock()
			f.Size = size
			if err != nil {
				log.Println("fetching:", f.URL, "error:", err)
				f.State = Error
				continue
			}
			f.State = InFlight
			return true
		}
	}
	return false
}

func (m *Manager) fetch(url_ string, statusC chan<- status) (int64, error) {
	dest := strings.TrimPrefix(url_, m.base)
	if s, err := url.QueryUnescape(dest); err == nil {
		dest = s
	}
	dest = filepath.ToSlash(dest)
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return 0, err
	}
	state, err := fetch.Fetch(dest, url_, &fetch.Options{
		BlockSize:       1 << 20, // (1MB)
		RequestPreparer: m.rp,
	})
	if err != nil {
		return 0, err
	}
	s := <-state
	size := s.Size
	go func() {
		for s := range state {
			statusC <- status{url_, s}
		}
	}()
	return size, nil
}

func (m *Manager) fileURLs(base string) ([]string, error) {
	files, err := m.ls(base)
	if err != nil {
		return nil, err
	}
	var urls []string
	for _, f := range files {
		if strings.HasSuffix(f, "/") {
			urls2, err := m.fileURLs(base + f)
			if err != nil {
				return nil, err
			}
			urls = append(urls, urls2...)
			continue
		}
		urls = append(urls, base+f)
	}
	return urls, nil
}

func (m *Manager) ls(url string) ([]string, error) {
	index, err := m.get(url)
	if err != nil {
		return nil, err
	}
	var files []string
	index.Find(m.selector).Each(func(_ int, v *goquery.Selection) {
		href, _ := v.Attr("href")
		if href == "../" {
			return
		}
		files = append(files, href)
	})
	return files, nil
}

func (m *Manager) get(url string) (*goquery.Document, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if m.rp != nil {
		m.rp(req)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return goquery.NewDocumentFromReader(res.Body)
}

func readCache(filename string, state interface{}) error {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(state)
}

func writeCache(filename string, state interface{}) error {
	tmp := filename + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	defer f.Close()
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, filename)
}
