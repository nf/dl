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
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nf/dl/fetch"
)

var (
	username = flag.String("user", "", "basic auth username")
	password = flag.String("pass", "", "basic auth password")
)

const defaultDest = "index.html"

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %v [flags] url...\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
	}
	for _, url := range args {
		if err := get(url); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
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

	rp := func(r *http.Request) {
		if *username != "" || *password != "" {
			r.SetBasicAuth(*username, *password)
		}
	}
	size, state, err := fetch.Fetch(dest, source, rp)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}

	var (
		lastState fetch.State
		lastCount int64
		ticker    = time.NewTicker(time.Second)
	)
	defer ticker.Stop()
	for {
		select {
		case s, ok := <-state:
			if !ok {
				log.Printf("%v bytes received",
					humanize.Comma(lastState.Count))
				return lastState.Err
			}
			lastState = s
		case <-ticker.C:
			log.Printf(
				"%v/%v bytes received (%v/sec)",
				humanize.Comma(lastState.Count),
				humanize.Comma(size),
				humanize.Bytes(uint64(lastState.Count-lastCount)),
			)
			lastCount = lastState.Count
		}
	}
}
