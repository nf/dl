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

package fetch

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
)

var (
	Concurrency = 10       // concurrent downloads
	Blocksize   = 10 << 20 // block size
)

type RequestPreparer func(*http.Request)

type State struct {
	Count int64
	Err   error
}

func Fetch(dest, source string, rp RequestPreparer) (size int64, s <-chan State, err error) {
	size, err = getSize(source, rp)
	if err != nil {
		return 0, nil, err
	}
	if size == 0 {
		return 0, nil, nil
	}

	out, err := os.Create(dest)
	if err != nil {
		return 0, nil, err
	}

	offsets := make(chan int64)
	go func() {
		for i := int64(0); i < size; i += int64(Blocksize) {
			offsets <- i
		}
		close(offsets)
	}()

	var (
		counts = make(chan int)
		errc   = make(chan error)
		wg     sync.WaitGroup
	)
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for off := range offsets {
				err := errors.New("sentinel")
				for fail := 0; err != nil && fail < 3; fail++ {
					err = getChunk(out, source, off, size, counts, rp)
				}
				if err != nil {
					errc <- fmt.Errorf("fetching chunk at offset %v: %v", off, err)
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(counts)
	}()

	state := make(chan State, 1)
	go func() {
		defer close(state)

		var count int64
		for {
			select {
			case n, ok := <-counts:
				count += int64(n)
				if !ok {
					err := out.Close()
					state <- State{count, err}
					return
				}
				state <- State{count, nil}
			case err := <-errc:
				out.Close()
				state <- State{count, err}
				return
			}
		}
	}()

	return size, state, nil
}

func getSize(url string, rp RequestPreparer) (int64, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return 0, fmt.Errorf("newrequest error: %v", err)
	}
	if rp != nil {
		rp(req)
	}
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return 0, fmt.Errorf("roundtrip error: %v", err)
	}
	size, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parseInt error: %v", err)
	}
	if res.Header.Get("Accept-Ranges") != "bytes" {
		return 0, errors.New("ranges not supported")
	}
	return size, nil
}

func getChunk(w io.WriterAt, url string, off, size int64, counts chan int, rp RequestPreparer) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("newrequest error: %v", err)
	}
	if rp != nil {
		rp(req)
	}
	end := off + int64(Blocksize) - 1
	if end > size-1 {
		end = size - 1
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", off, end))

	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("roundtrip error: %v", err)
	}
	if res.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("bad status: %v", res.Status)
	}
	wr := fmt.Sprintf("bytes %v-%v/%v", off, end, size)
	if cr := res.Header.Get("Content-Range"); cr != wr {
		res.Body.Close()
		return fmt.Errorf("bad content-range: %v", cr)
	}

	_, err = io.Copy(&sectionWriter{w, off}, logReader{res.Body, counts})
	res.Body.Close()
	if err != nil {
		return fmt.Errorf("copy error: %v", err)
	}

	return nil
}

type sectionWriter struct {
	w   io.WriterAt
	off int64
}

func (w *sectionWriter) Write(b []byte) (n int, err error) {
	n, err = w.w.WriteAt(b, w.off)
	w.off += int64(n)
	return
}

type logReader struct {
	r  io.Reader
	ch chan int
}

func (r logReader) Read(b []byte) (n int, err error) {
	n, err = r.r.Read(b)
	r.ch <- n
	return
}
