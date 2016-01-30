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
)

var (
	DefaultConcurrency    = 10       // concurrent downloads
	DefaultBlockSize      = 10 << 20 // block size (10MB)
	DefaultErrorThreshold = 10       // number of retries to make per file
)

type Options struct {
	// Concurrency is the number of simultaneous requests to use.
	Concurrency int

	// BlockSize is the size of each chunk.
	BlockSize int64

	// ErrorThreshold is the number of HTTP errors to tolerate.
	ErrorThreshold int

	RequestPreparer RequestPreparer
}

func (o *Options) applyDefaults() {
	if o.Concurrency == 0 {
		o.Concurrency = DefaultConcurrency
	}
	if o.BlockSize == 0 {
		o.BlockSize = int64(DefaultBlockSize)
	}
	if o.ErrorThreshold == 0 {
		o.ErrorThreshold = DefaultErrorThreshold
	}
}

// RequestPreparer is used by Fetch to prepare each HTTP request.
// It is typically used to set basic authentication or cookies.
type RequestPreparer func(*http.Request)

type State struct {
	Size, Downloaded int64
	Failures         int
	Error            error

	// Pending and Complete blocks; Pending includes Inflight blocks.
	Pending, Complete []Block
	// Blocks being downloaded presently.
	Inflight []Block
}

type Block struct {
	Offset, Length int64
}

func Fetch(dest, source string, opts *Options) (s <-chan State, err error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	o.applyDefaults()

	size, err := o.getSize(source)
	if err != nil {
		return nil, err
	}

	out, err := os.Create(dest)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, out.Close()
	}

	var pending []Block
	for offset := int64(0); offset < size; offset += int64(o.BlockSize) {
		length := int64(o.BlockSize)
		if offset+length > size {
			length = size - offset
		}
		pending = append(pending, Block{offset, length})
	}
	return o.fetch(out, source, size, pending, nil)
}

func Resume(dest, source string, opts *Options, initState State) (s <-chan State, err error) {
	var o Options
	if opts != nil {
		o = *opts
	}
	o.applyDefaults()

	size, err := o.getSize(source)
	if err != nil {
		return nil, err
	}
	if size != initState.Size {
		return nil, fmt.Errorf("remote reported size=%v, expected %v", size, initState.Size)
	}
	if len(initState.Pending) == 0 {
		return nil, nil
	}
	out, err := os.OpenFile(dest, os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return o.fetch(out, source, size, initState.Pending, initState.Complete)

}

type file interface {
	io.WriterAt
	io.Closer
}

func (o *Options) fetch(out file, source string, size int64, pending, complete []Block) (s <-chan State, err error) {
	var (
		inflight   = map[Block]bool{}
		downloaded int64
		failures   int
	)
	for _, b := range complete {
		downloaded += b.Length
	}

	type failure struct {
		Block
		error
	}
	state := make(chan State, 1)
	go func() {
		defer close(state)

		var (
			throttle   = make(chan bool, o.Concurrency)
			throttleCh = throttle // nil'd to prevent select spin
			failureCh  = make(chan failure)
			successCh  = make(chan Block)
		)
		report := func(err error) {
			inflightSlice := make([]Block, 0, len(inflight))
			for b := range inflight {
				inflightSlice = append(inflightSlice, b)
			}
			state <- State{
				Size:       size,
				Downloaded: downloaded,
				Pending:    append(inflightSlice, pending...),
				Inflight:   inflightSlice,
				Complete:   append([]Block{}, complete...),
				Failures:   failures,
				Error:      err,
			}
		}
		report(nil)
		for {
			if len(pending) == 0 {
				throttleCh = nil
			} else {
				throttleCh = throttle
			}
			select {
			case throttleCh <- true:
				b := pending[0]
				pending = pending[1:]
				inflight[b] = true
				report(nil)
				go func() {
					defer func() { <-throttle }()
					err := o.getBlock(out, source, b, size)
					if err != nil {
						failureCh <- failure{b, err}
					} else {
						successCh <- b
					}
				}()
			case b := <-successCh:
				delete(inflight, b)
				downloaded += b.Length
				complete = append(complete, b)
				report(nil)
				if downloaded == size {
					return
				}
			case fail := <-failureCh:
				failures++
				if failures > o.ErrorThreshold {
					out.Close()
					report(fail.error)
					return
				}
				// Re-queue this block for download.
				delete(inflight, fail.Block)
				pending = append(pending, fail.Block)
				report(nil)
			}
		}
	}()

	return state, nil
}

func (o *Options) getSize(url string) (int64, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return 0, fmt.Errorf("newrequest error: %v", err)
	}
	if rp := o.RequestPreparer; rp != nil {
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

func (o *Options) getBlock(w io.WriterAt, url string, b Block, size int64) error {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("newrequest error: %v", err)
	}
	if rp := o.RequestPreparer; rp != nil {
		rp(req)
	}
	end := b.Offset + b.Length
	if end > size-1 {
		end = size - 1
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", b.Offset, end))

	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return fmt.Errorf("roundtrip error: %v", err)
	}
	if res.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("bad status: %v", res.Status)
	}
	wr := fmt.Sprintf("bytes %v-%v/%v", b.Offset, end, size)
	if cr := res.Header.Get("Content-Range"); cr != wr {
		res.Body.Close()
		return fmt.Errorf("bad content-range: %v (want %v)", cr, wr)
	}

	_, err = io.Copy(&sectionWriter{w, b.Offset}, res.Body)
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
