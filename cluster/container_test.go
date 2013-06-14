// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"bytes"
	"github.com/dotcloud/docker"
	dclient "github.com/fsouza/go-dockerclient"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
)

func TestCreateContainer(t *testing.T) {
	body := `{"Id":"e90302"}`
	handler := []bool{false, false}
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler[0] = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler[1] = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864}
	id, container, err := cluster.CreateContainer(&config)
	if err != nil {
		t.Fatal(err)
	}
	if id != "handler0" {
		t.Errorf("CreateContainer: ran on wrong node. Want %q. Got %q.", "handler0", id)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
}

func TestInspectContainer(t *testing.T) {
	body := `{"Id":"e90302","Path":"date","Args":[]}`
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	server3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server3.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
		Node{ID: "handler2", Address: server3.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "e90302"
	container, err := cluster.InspectContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if container.ID != id {
		t.Errorf("InspectContainer(%q): Wrong ID. Want %q. Got %q.", id, id, container.ID)
	}
	if container.Path != "date" {
		t.Errorf("InspectContainer(%q): Wrong Path. Want %q. Got %q.", id, "date", container.Path)
	}
}

func TestInspectContainerNoSuchContainer(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "e90302"
	container, err := cluster.InspectContainer(id)
	if container != nil {
		t.Errorf("InspectContainer(%q): Expected <nil> container, got %#v.", id, container)
	}
	if err != dclient.ErrNoSuchContainer {
		t.Errorf("InspectContainer(%q): Wrong error. Want %#v. Got %#v.", id, dclient.ErrNoSuchContainer, err)
	}
}

func TestInspectContainerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusInternalServerError)
	}))
	defer server.Close()
	cluster, err := New(Node{ID: "handler0", Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	id := "a2033"
	container, err := cluster.InspectContainer(id)
	if container != nil {
		t.Errorf("InspectContainer(%q): Expected <nil> container, got %#v.", id, container)
	}
	if err == nil {
		t.Errorf("InspectContainer(%q): Expected non-nil error, got <nil>", id)
	}
}

func TestKillContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.KillContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("KillContainer(%q): Did not call node http server", id)
	}
}

func TestListContainers(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `[
     {
             "Id": "8dfafdbc3a40",
             "Image": "base:latest",
             "Command": "echo 1",
             "Created": 1367854155,
             "Status": "Exit 0"
     }
]`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `[
     {
             "Id": "3176a2479c92",
             "Image": "base:latest",
             "Command": "echo 3333333333333333",
             "Created": 1367854154,
             "Status": "Exit 0"
     },
     {
             "Id": "9cd87474be90",
             "Image": "base:latest",
             "Command": "echo 222222",
             "Created": 1367854155,
             "Status": "Exit 0"
     }
]`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	server3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `[]`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server3.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
		Node{ID: "handler2", Address: server3.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := containerList([]docker.APIContainers{
		{ID: "3176a2479c92", Image: "base:latest", Command: "echo 3333333333333333", Created: 1367854154, Status: "Exit 0"},
		{ID: "9cd87474be90", Image: "base:latest", Command: "echo 222222", Created: 1367854155, Status: "Exit 0"},
		{ID: "8dfafdbc3a40", Image: "base:latest", Command: "echo 1", Created: 1367854155, Status: "Exit 0"},
	})
	sort.Sort(expected)
	containers, err := cluster.ListContainers(dclient.ListContainersOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := containerList(containers)
	sort.Sort(got)
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("ListContainers: Wrong containers. Want %#v. Got %#v.", expected, got)
	}
}

func TestListContainersFailure(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `[
     {
             "Id": "8dfafdbc3a40",
             "Image": "base:latest",
             "Command": "echo 1",
             "Created": 1367854155,
             "Status": "Exit 0"
     }
]`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Internal failure", http.StatusInternalServerError)
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := []docker.APIContainers{
		{ID: "8dfafdbc3a40", Image: "base:latest", Command: "echo 1", Created: 1367854155, Status: "Exit 0"},
	}
	containers, err := cluster.ListContainers(dclient.ListContainersOptions{})
	if err == nil {
		t.Error("ListContainers: Expected non-nil error, got <nil>")
	}
	if !reflect.DeepEqual(containers, expected) {
		t.Errorf("ListContainers: Want %#v. Got %#v.", expected, containers)
	}
}

func TestRemoveContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.RemoveContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("RemoveContainer(%q): Did not call node HTTP server", id)
	}
}

func TestStartContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.StartContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("StartContainer(%q): Did not call node HTTP server", id)
	}
}

func TestStopContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.StopContainer(id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("StopContainer(%q, 10): Did not call node HTTP server", id)
	}
}

func TestRestartContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.RestartContainer(id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("RestartContainer(%q, 10): Did not call node HTTP server", id)
	}
}

func TestWaitContainer(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `{"StatusCode":34}`
		w.Write([]byte(body))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	expected := 34
	status, err := cluster.WaitContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if status != expected {
		t.Errorf("WaitContainer(%q): Wrong status. Want %d. Got %d.", id, expected, status)
	}
}

func TestAttachToContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "container not found", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte("something happened"))
	}))
	defer server2.Close()
	cluster, err := New(
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := dclient.AttachToContainerOptions{
		Container:    "abcdef",
		OutputStream: &bytes.Buffer{},
		Logs:         true,
		Stdout:       true,
	}
	err = cluster.AttachToContainer(opts)
	if err != nil {
		t.Errorf("AttachToContainer: unexpected error. Want <nil>. Got %#v.", err)
	}
	if !called {
		t.Error("AttachToContainer: Did not call the remote HTTP API")
	}
}
