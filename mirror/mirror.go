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
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/nf/dl/fetch"
)

var (
	baseURL  = flag.String("base", "", "base URL to mirror")
	username = flag.String("user", "", "basic auth username")
	password = flag.String("pass", "", "basic auth password")
	refresh  = flag.Duration("refresh", time.Minute, "refresh interval")
)

func main() {
	flag.Parse()
	NewManager(
		*baseURL,
		*refresh,
		func(r *http.Request) {
			if *username != "" || *password != "" {
				r.SetBasicAuth(*username, *password)
			}
		},
	).Run()
}

type State int

const (
	New State = iota
	InFlight
	Done
)

func NewManager(baseURL string, refresh time.Duration, rp fetch.RequestPreparer) *Manager {
	return &Manager{
		base:    baseURL,
		files:   make(map[string]State),
		refresh: refresh,
		rp:      rp,
	}
}

type Manager struct {
	base    string
	files   map[string]State // keyed by remote URL
	refresh time.Duration
	rp      fetch.RequestPreparer
}

func (m *Manager) Run() {
	if err := m.update(); err != nil {
		log.Fatal("update:", err)
	}
	done := make(chan string)
	fetching := m.fetchNext(done)

	refresh := time.NewTicker(m.refresh)
	defer refresh.Stop()
	for {
		select {
		case <-refresh.C:
			if err := m.update(); err != nil {
				log.Println("update:", err)
			}
			if !fetching {
				fetching = m.fetchNext(done)
			}
		case url := <-done:
			m.files[url] = Done
			fetching = m.fetchNext(done)
		}
	}
}

func (m *Manager) update() error {
	urls, err := m.fileURLs(m.base)
	if err != nil {
		return err
	}
	for _, u := range urls {
		if _, ok := m.files[u]; !ok {
			m.files[u] = New
		}
	}
	return nil
}

func (m *Manager) fetchNext(done chan string) bool {
	for url, state := range m.files {
		if state == New {
			m.files[url] = InFlight
			go m.fetch(url, done)
			return true
		}
	}
	return false
}

func (m *Manager) fetch(url string, done chan string) {
	log.Println("fetching:", url)
	defer func() { done <- url }()
	dest := filepath.ToSlash(strings.TrimPrefix(url, m.base))
	if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
		log.Printf("%v: mkdir: %v", url, err)
		return
	}
	_, state, err := fetch.Fetch(dest, url, m.rp)
	if err == nil {
		for s := range state {
			if s.Err != nil {
				err = s.Err
				break
			}
		}
	}
	if err != nil {
		log.Printf("fetching: %v: error: %v", url, err)
	}
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
