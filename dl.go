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

// Command dl downloads the specified file in chunks,
// using multiple concurrent HTTP requests.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

var (
	username    = flag.String("user", "", "basic auth username")
	password    = flag.String("pass", "", "basic auth password")
	concurrency = flag.Int("n", 10, "concurrent downloads")
	blocksize   = flag.Int("bs", 10<<20, "block size")
)

const defaultDest = "index.html"

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %v [flags] <url>\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	url := flag.Arg(0)
	if url == "" {
		flag.Usage()
	}
	if err := get(url); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func get(source string) error {
	u, err := url.Parse(source)
	if err != nil {
		return err
	}
	dest := path.Base(u.Path)
	if dest == "" {
		log.Println("using default dest filename:", defaultDest)
		dest = defaultDest
	}

	size, err := getSize(source)
	if err != nil {
		return err
	}
	log.Printf("Total size: %v bytes", humanize.Comma(size))
	if size == 0 {
		log.Println("nothing to do!")
		return nil
	}

	out, err := os.Create(dest)
	if err != nil {
		return err
	}

	offsets := make(chan int64)
	go func() {
		for i := int64(0); i < size; i += int64(*blocksize) {
			offsets <- i
		}
		close(offsets)
	}()

	var (
		counts = make(chan int)
		errc   = make(chan error)
		wg     sync.WaitGroup
	)
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			for off := range offsets {
				if err := getChunk(out, source, off, size, counts); err != nil {
					errc <- err
				}
			}
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(counts)
	}()

	var (
		received  int64
		lastCount int64
		lastPrint = time.Now()
	)
	for {
		select {
		case n, ok := <-counts:
			received += int64(n)
			if n := time.Now(); n.Sub(lastPrint) > time.Second || !ok {
				log.Printf(
					"%v/%v bytes received (%v/sec)",
					humanize.Comma(received),
					humanize.Comma(size),
					humanize.Bytes(uint64(received-lastCount)),
				)
				lastPrint = n
				lastCount = received
			}
			if !ok {
				return out.Close()
			}
		case err := <-errc:
			out.Close()
			return err
		}
	}
}

func getSize(url string) (int64, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return 0, fmt.Errorf("newrequest error: %v", err)
	}
	if *username != "" || *password != "" {
		req.SetBasicAuth(*username, *password)
	}
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return 0, fmt.Errorf("roundtrip error: %v", err)
	}
	size, err := strconv.ParseInt(res.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parseInt error: %v", err)
	}
	return size, nil
}

func getChunk(w io.WriterAt, url string, off, size int64, counts chan int) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("newrequest error: %v", err)
	}
	if *username != "" || *password != "" {
		req.SetBasicAuth(*username, *password)
	}
	end := off + int64(*blocksize) - 1
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
