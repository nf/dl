package fetch

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestFetch(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	ts := httptest.NewServer(http.FileServer(http.Dir("testdata")))
	defer ts.Close()

	dst := filepath.Join(tmpdir, "testfile")
	state, err := Fetch(dst, ts.URL+"/testfile", &Options{
		Concurrency: 4,
		BlockSize:   8,
	})
	if err != nil {
		t.Fatal(err)
	}

	for s := range state {
		t.Logf("%#v", s)
		if s.Error != nil {
			t.Fatal(err)
		}
	}

	compare(t, dst)
}

func TestResume(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	fh := &failingHandler{
		http.FileServer(http.Dir("testdata")),
		3,
	}
	ts := httptest.NewServer(fh)
	defer ts.Close()

	dst := filepath.Join(tmpdir, "testfile")
	opts := &Options{Concurrency: 4, BlockSize: 8, ErrorThreshold: 1}
	state, err := Fetch(dst, ts.URL+"/testfile", opts)
	if err != nil {
		t.Fatal(err)
	}

	var lastState State
	for s := range state {
		logBlocks(t, "fetch pending", s.Pending)
		if s.Error != nil {
			err = s.Error
			t.Logf("fetch error: %v", err)
		}
		lastState = s
	}
	if err == nil {
		t.Fatal("fetch didn't return the expected error")
	}

	fh.okRequestsLeft = -1 // Should succeed now.

	state, err = Resume(dst, ts.URL+"/testfile", opts, lastState)
	if err != nil {
		t.Fatal(err)
	}

	for s := range state {
		logBlocks(t, "resume pending", s.Pending)
		if s.Error != nil {
			t.Fatal(s.Error)
		}
	}

	compare(t, dst)
}

func logBlocks(t *testing.T, name string, bs []Block) {
	var buf bytes.Buffer
	for i, b := range bs {
		if i > 0 {
			fmt.Fprint(&buf, " ")
		}
		fmt.Fprintf(&buf, "%v(%v)", b.Offset, b.Length)
	}
	t.Logf("%v: %v", name, &buf)
}

func compare(t *testing.T, dst string) {
	want, err := ioutil.ReadFile(filepath.Join("testdata", "testfile"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := ioutil.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("content mismatch. got %q, want %q", got, want)
	}
}

type failingHandler struct {
	http.Handler
	okRequestsLeft int
}

func (h *failingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.okRequestsLeft == 0 {
		http.Error(w, "out of requests", 500)
		return
	}
	h.okRequestsLeft--
	h.Handler.ServeHTTP(w, r)
}
