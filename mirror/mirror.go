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
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/nf/dl/fetch"
)

var (
	baseURL  = flag.String("base", "", "base URL to mirror")
	username = flag.String("user", "", "basic auth username")
	password = flag.String("pass", "", "basic auth password")
	refresh  = flag.Duration("refresh", time.Minute, "refresh interval")
	httpAddr = flag.String("http", "localhost:8080", "HTTP listen address")
)

func main() {
	flag.Parse()
	m := NewManager(
		*baseURL,
		*refresh,
		func(r *http.Request) {
			if *username != "" || *password != "" {
				r.SetBasicAuth(*username, *password)
			}
		},
	)
	go m.Run()
	http.Handle("/", m)
	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}

type File struct {
	URL            string
	Received, Size int64
	State          State
}

func (f *File) Name() string {
	u, err := url.Parse(f.URL)
	if err != nil {
		return f.URL
	}
	return path.Base(u.Path)
}

func (f *File) PercentDone() int {
	if f.Received == 0 {
		return 0
	}
	return int(float64(f.Received) / float64(f.Size) * 100)
}

type State int

const (
	New State = iota
	StartNow
	InFlight
	Done
	Error
)

func (s State) String() string {
	return map[State]string{
		New:      "New",
		StartNow: "Starting",
		InFlight: "Downloading",
		Done:     "Done",
		Error:    "Error",
	}[s]
}

func NewManager(baseURL string, refresh time.Duration, rp fetch.RequestPreparer) *Manager {
	return &Manager{
		base:    baseURL,
		files:   make(map[string]*File),
		refresh: refresh,
		rp:      rp,
		start:   make(chan string),
	}
}

type Manager struct {
	base    string
	refresh time.Duration
	rp      fetch.RequestPreparer

	filesMu sync.RWMutex
	files   map[string]*File // keyed by remote URL

	start chan string
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s := r.FormValue("start"); r.Method == "POST" && s != "" {
		m.start <- s
		http.Redirect(w, r, "", http.StatusFound)
		return
	}
	var b bytes.Buffer
	m.filesMu.RLock()
	err := uiTemplate.Execute(&b, m.files)
	m.filesMu.RUnlock()
	if err != nil {
		log.Println(err)
	}
	b.WriteTo(w)
}

type status struct {
	url string
	fetch.State
}

func (m *Manager) Run() {
	if err := m.update(); err != nil {
		log.Fatal("update:", err)
	}
	status := make(chan status)
	fetching := m.fetchNext(status)

	refresh := time.NewTicker(m.refresh)
	defer refresh.Stop()
	for {
		select {
		case <-refresh.C:
			if err := m.update(); err != nil {
				log.Println("update:", err)
			}
			if !fetching {
				fetching = m.fetchNext(status)
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
			f.Received = s.Count
			done := false
			switch {
			case s.Err != nil:
				log.Println("fetching:", f.URL, "error:", s.Err)
				f.State = Error
				done = true
			case s.Count == f.Size:
				if f.State != Done {
					f.State = Done
					done = true
				}
			}
			m.filesMu.Unlock()
			if done {
				fetching = m.fetchNext(status)
			}
		case u := <-m.start:
			m.filesMu.Lock()
			f, ok := m.files[u]
			if !ok {
				log.Printf("start of unknown url %q", u)
				m.filesMu.Unlock()
				break
			}
			f.State = StartNow
			m.filesMu.Unlock()
			if !fetching {
				fetching = m.fetchNext(status)
			}
		}
	}
}

func (m *Manager) update() error {
	urls, err := m.fileURLs(m.base)
	if err != nil {
		return err
	}
	m.filesMu.Lock()
	for _, u := range urls {
		if _, ok := m.files[u]; !ok {
			m.files[u] = &File{URL: u}
		}
	}
	m.filesMu.Unlock()
	return nil
}

func (m *Manager) fetchNext(status chan<- status) bool {
	m.filesMu.Lock()
	defer m.filesMu.Unlock()
	for _, f := range m.files {
		if f.State == StartNow || f.State == New && m.canStart() {
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

func (m *Manager) canStart() bool {
	// TODO(adg): implement auto-start at 2am
	return false
}

func (m *Manager) fetch(url string, statusC chan<- status) (int64, error) {
	dest := filepath.ToSlash(strings.TrimPrefix(url, m.base))
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		return 0, err
	}
	size, state, err := fetch.Fetch(dest, url, m.rp)
	if err != nil {
		return size, err
	}
	go func() {
		for s := range state {
			statusC <- status{url, s}
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
	index.Find("td > a").Each(func(_ int, v *goquery.Selection) {
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
