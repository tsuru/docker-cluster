// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"bytes"
	"github.com/fsouza/go-dockerclient"
	"github.com/globocom/tsuru/safe"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
)

func TestRemoveImage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	name := "tsuru/python"
	err = cluster.RemoveImage(name)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("RemoveImage(%q): Did not call node HTTP server", name)
	}
}

func TestRemoveImageNotFound(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	name := "tsuru/python"
	err = cluster.RemoveImage(name)
	if err != docker.ErrNoSuchImage {
		t.Errorf("RemoveImage(%q): wrong error. Want %#v. Got %#v.", name, docker.ErrNoSuchImage, err)
	}
}

func TestPullImage(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 1!"))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 2!"))
	}))
	defer server2.Close()
	var buf safe.Buffer
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.PullImage(docker.PullImageOptions{Repository: "tsuru/python"}, &buf)
	if err != nil {
		t.Error(err)
	}
	re := regexp.MustCompile(`^Pulling from \d`)
	if !re.MatchString(buf.String()) {
		t.Errorf("Wrong output: Want %q. Got %q.", "Pulling from [12]", buf.String())
	}
}

func TestPullImageNotFound(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	err = cluster.PullImage(docker.PullImageOptions{Repository: "tsuru/python"}, &buf)
	if err == nil {
		t.Error("PullImage: got unexpected <nil> error")
	}
}

func TestPushImage(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pushing to server 1!"))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pushing to server 2!"))
	}))
	defer server2.Close()
	var buf safe.Buffer
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.PushImage(docker.PushImageOptions{Name: "tsuru/ruby"}, &buf)
	re := regexp.MustCompile(`^Pushing to server \d`)
	if !re.MatchString(buf.String()) {
		t.Errorf("Wrong output: Want %q. Got %q.", "Pushing to server [12]", buf.String())
	}
}

func TestPushImageNotFound(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	err = cluster.PushImage(docker.PushImageOptions{Name: "tsuru/python"}, &buf)
	if err == nil {
		t.Error("PushImage: got unexpected <nil> error")
	}
}

func TestPushImageWithStorage(t *testing.T) {
	var count int
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pushed"))
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	storage := mapStorage{iMap: map[string]string{"tsuru/python": "handler1"}}
	cluster.SetStorage(&storage)
	var buf bytes.Buffer
	err = cluster.PushImage(docker.PushImageOptions{Name: "tsuru/python"}, &buf)
	if err != nil {
		t.Error(err)
	}
	if count > 0 {
		t.Error("PushImage with storage: should not send request to all servers, but did.")
	}
}
