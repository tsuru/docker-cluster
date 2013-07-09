// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	dclient "github.com/fsouza/go-dockerclient"
	"github.com/globocom/tsuru/safe"
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
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864}
	container, err := cluster.CreateContainer(&config)
	if err != nil {
		t.Fatal(err)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
}

func TestCreateContainerWithStorage(t *testing.T) {
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
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var storage mapStorage
	cluster.SetStorage(&storage)
	config := docker.Config{Memory: 67108864}
	_, err = cluster.CreateContainer(&config)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"e90302": "handler0"}
	if storage.m["e90302"] != "handler0" {
		t.Errorf("Cluster.CreateContainer() with storage: wrong data. Want %#v. Got %#v.", expected, storage.m)
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
	cluster, err := New(nil,
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

func TestInspectContainerWithStorage(t *testing.T) {
	body := `{"Id":"e90302","Path":"date","Args":[]}`
	var count int
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "e90302"
	storage := mapStorage{m: map[string]string{id: "handler1"}}
	cluster.SetStorage(&storage)
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
	if count > 0 {
		t.Error("InspectContainer(%q) with storage: should not send request to all servers, but did.", "e90302")
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
	cluster, err := New(nil,
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
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("InspectContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestInspectContainerNoSuchContainerWithStorage(t *testing.T) {
	cluster, err := New(nil, Node{ID: "handler0", Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.SetStorage(&mapStorage{})
	id := "e90302"
	container, err := cluster.InspectContainer(id)
	if container != nil {
		t.Errorf("InspectContainer(%q): Expected <nil> container, got %#v.", id, container)
	}
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("InspectContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestInspectContainerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusInternalServerError)
	}))
	defer server.Close()
	cluster, err := New(nil, Node{ID: "handler0", Address: server.URL})
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
	cluster, err := New(nil,
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

func TestKillContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	storage := mapStorage{m: map[string]string{id: "handler1"}}
	cluster.SetStorage(&storage)
	err = cluster.KillContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("KillContainer(%q): should not call the node server", id)
	}
}

func TestKillContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	cluster.SetStorage(&mapStorage{})
	err = cluster.KillContainer(id)
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("KillContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	cluster, err := New(nil,
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
	cluster, err := New(nil,
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
	cluster, err := New(nil,
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

func TestRemoveContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	storage := mapStorage{m: map[string]string{id: "handler1"}}
	cluster.SetStorage(&storage)
	err = cluster.RemoveContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("RemoveContainer(%q): should not call the node server", id)
	}
}

func TestRemoveContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	cluster.SetStorage(&mapStorage{})
	err = cluster.RemoveContainer(id)
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("RemoveContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	cluster, err := New(nil,
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

func TestStartContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	storage := mapStorage{m: map[string]string{id: "handler1"}}
	cluster.SetStorage(&storage)
	err = cluster.StartContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("StartContainer(%q): should not call the node server", id)
	}
}

func TestStartContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	cluster.SetStorage(&mapStorage{})
	err = cluster.StartContainer(id)
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("StartContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	cluster, err := New(nil,
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

func TestStopContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	storage := mapStorage{m: map[string]string{id: "handler1"}}
	cluster.SetStorage(&storage)
	err = cluster.StopContainer(id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("StopContainer(%q): should not call the node server", id)
	}
}

func TestStopContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	cluster.SetStorage(&mapStorage{})
	err = cluster.StopContainer(id, 10)
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("StopContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	cluster, err := New(nil,
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
	cluster, err := New(nil,
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
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := dclient.AttachToContainerOptions{
		Container:    "abcdef",
		OutputStream: &safe.Buffer{},
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

func TestCommitContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "container not found", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte(`{"Id":"596069db4bf5"}`))
	}))
	defer server2.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := dclient.CommitContainerOptions{
		Container: "abcdef",
	}
	image, err := cluster.CommitContainer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if image.ID != "596069db4bf5" {
		t.Errorf("CommitContainer: the image container is %s, expected: '596069db4bf5'", image.ID)
	}
	if !called {
		t.Error("CommitContainer: Did not call the remote HTTP API")
	}
}

func TestCommitContainerError(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "container not found", http.StatusNotFound)
	}))
	defer server1.Close()
	cluster, err := New(nil,
		Node{ID: "handler0", Address: server1.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := dclient.CommitContainerOptions{
		Container: "abcdef",
	}
	image, err := cluster.CommitContainer(opts)
	if err == nil {
		t.Fatal(err)
	}
	if image != nil {
		t.Errorf("CommitContainerError: the image should be nil but it is %s", image.ID)
	}
}

func TestGetNode(t *testing.T) {
	cluster, err := New(nil,
		Node{ID: "handler0", Address: "http://localhost:4243"},
		Node{ID: "handler1", Address: "http://localhost:4242"},
		Node{ID: "handler2", Address: "http://localhost:4241"},
	)
	if err != nil {
		t.Fatal(err)
	}
	var storage mapStorage
	storage.Store("e90301", "handler1")
	storage.Store("e90304", "handler1")
	storage.Store("e90303", "handler2")
	storage.Store("e90302", "handler3")
	cluster.SetStorage(&storage)
	_, err = cluster.getNode("e90302")
	if err != ErrUnknownNode {
		t.Errorf("cluster.getNode(%q): wrong error. Want %#v. Got %#v.", "e90302", ErrUnknownNode, err)
	}
	node, err := cluster.getNode("e90301")
	if err != nil {
		t.Error(err)
	}
	if node.id != "handler1" {
		t.Errorf("cluster.getNode(%q): wrong node. Want %q. Got %q.", "e90301", "handler1", node.id)
	}
	_, err = cluster.getNode("e90305")
	expected := dclient.NoSuchContainer{ID: "e90305"}
	if !reflect.DeepEqual(err, &expected) {
		t.Errorf("cluster.getNode(%q): wrong error. Want %#v. Got %#v.", "e90305", expected, err)
	}
}
