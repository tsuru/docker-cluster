// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"bytes"
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
	cluster, err := New(nil, nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864}
	nodeID, container, err := cluster.CreateContainer(dclient.CreateContainerOptions{}, &config)
	if err != nil {
		t.Fatal(err)
	}
	if nodeID != "handler0" {
		t.Errorf("CreateContainer: wrong node  ID. Want %q. Got %q.", "handler0", nodeID)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
}

func TestCreateContainerOptions(t *testing.T) {
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
	cluster, err := New(nil, nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864}
	opts := dclient.CreateContainerOptions{Name: "name"}
	nodeID, container, err := cluster.CreateContainer(opts, &config)
	if err != nil {
		t.Fatal(err)
	}
	if nodeID != "handler0" {
		t.Errorf("CreateContainer: wrong node  ID. Want %q. Got %q.", "handler0", nodeID)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
}

func TestCreateContainerFailure(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "NoSuchImage", http.StatusNotFound)
	}))
	defer server1.Close()
	cluster, err := New(nil, nil, Node{ID: "handler0", Address: server1.URL})
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864}
	_, _, err = cluster.CreateContainer(dclient.CreateContainerOptions{}, &config)
	if err == nil {
		t.Error("Got unexpected <nil> error")
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
	var storage mapStorage
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864}
	_, _, err = cluster.CreateContainer(dclient.CreateContainerOptions{}, &config)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{"e90302": "handler0"}
	if storage.cMap["e90302"] != "handler0" {
		t.Errorf("Cluster.CreateContainer() with storage: wrong data. Want %#v. Got %#v.", expected, storage.cMap)
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
	cluster, err := New(nil, nil,
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
	id := "e90302"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
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
		t.Errorf("InspectContainer(%q) with storage: should not send request to all servers, but did.", "e90302")
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
	cluster, err := New(nil, nil,
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
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:4243"})
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

func TestInspectContainerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusInternalServerError)
	}))
	defer server.Close()
	cluster, err := New(nil, nil, Node{ID: "handler0", Address: server.URL})
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
	cluster, err := New(nil, nil,
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
	id := "abc123"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.KillContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("KillContainer(%q): should not call the node server", id)
	}
}

func TestKillContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
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
	cluster, err := New(nil, nil,
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
	cluster, err := New(nil, nil,
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

func TestListContainersSchedulerFailure(t *testing.T) {
	cluster, err := New(failingScheduler{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	containers, err := cluster.ListContainers(dclient.ListContainersOptions{})
	expected := "Cannot retrieve list of nodes"
	if err.Error() != expected {
		t.Errorf("ListContainers(): wrong error. Want %q. Got %q", expected, err.Error())
	}
	if containers != nil {
		t.Errorf("ListContainers(): wrong result. Want <nil>. Got %#v.", containers)
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
	cluster, err := New(nil, nil,
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
	id := "abc123"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RemoveContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("RemoveContainer(%q): should not call the node server", id)
	}
	_, err = storage.RetrieveContainer(id)
	if err == nil {
		t.Errorf("RemoveContainer(%q): should remove the container from the storage", id)
	}
}

func TestRemoveContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
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
	cluster, err := New(nil, nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.StartContainer(id, nil)
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
	id := "abc123"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.StartContainer(id, nil)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("StartContainer(%q): should not call the node server", id)
	}
}

func TestStartContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.StartContainer(id, nil)
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
	cluster, err := New(nil, nil,
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
	id := "abc123"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.StopContainer(id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("StopContainer(%q): should not call the node server", id)
	}
}

func TestStopContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
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
	cluster, err := New(nil, nil,
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

func TestRestartContainerWithStorage(t *testing.T) {
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
	id := "abc123"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RestartContainer(id, 10)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("RestartContainer(%q): should not call the node server", id)
	}
}

func TestRestartContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.RestartContainer(id, 10)
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("RestartContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	cluster, err := New(nil, nil,
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

func TestWaitContainerNotFound(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(nil, nil,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	expected := -1
	status, err := cluster.WaitContainer(id)
	if err == nil {
		t.Errorf("WaitContainer(%q): unexpected <nil> error", id)
	}
	if status != expected {
		t.Errorf("WaitContainer(%q): Wrong status. Want %d. Got %d.", id, expected, status)
	}
}

func TestWaitContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "No such container", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `{"StatusCode":34}`
		w.Write([]byte(body))
	}))
	defer server2.Close()
	id := "abc123"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := 34
	status, err := cluster.WaitContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if status != expected {
		t.Errorf("WaitContainer(%q): Wrong status. Want %d. Got %d.", id, expected, status)
	}
	if called {
		t.Errorf("WaitContainer(%q): should not call the all node servers.", id)
	}
}

func TestWaitContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	expectedStatus := -1
	status, err := cluster.WaitContainer(id)
	if status != expectedStatus {
		t.Errorf("WaitContainer(%q): wrong status. Want %d. Got %d.", id, expectedStatus, status)
	}
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("WaitContainer(%q): wrong error. Want %#v. Got %#v.", id, expected, err)
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
	cluster, err := New(nil, nil,
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
		RawTerminal:  true,
	}
	err = cluster.AttachToContainer(opts)
	if err != nil {
		t.Errorf("AttachToContainer: unexpected error. Want <nil>. Got %#v.", err)
	}
	if !called {
		t.Error("AttachToContainer: Did not call the remote HTTP API")
	}
}

func TestAttachToContainerWithStorage(t *testing.T) {
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
	id := "abcdef"
	storage := mapStorage{cMap: map[string]string{id: "handler1"}}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := dclient.AttachToContainerOptions{
		Container:    id,
		OutputStream: &safe.Buffer{},
		Logs:         true,
		Stdout:       true,
	}
	err = cluster.AttachToContainer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("AttachToContainer(): should not call the node server")
	}
}

func TestAttachToContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abcdef"
	opts := dclient.AttachToContainerOptions{
		Container:    "abcdef",
		OutputStream: &safe.Buffer{},
		Logs:         true,
		Stdout:       true,
	}
	err = cluster.AttachToContainer(opts)
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("AttachToContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	cluster, err := New(nil, nil,
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
	cluster, err := New(nil, nil,
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

func TestCommitContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		http.Error(w, "container not found", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"Id":"596069db4bf5"}`))
	}))
	defer server2.Close()
	id := "abc123"
	storage := mapStorage{
		cMap: map[string]string{id: "handler1"},
		iMap: map[string]string{},
	}
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: server1.URL},
		Node{ID: "handler1", Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := dclient.CommitContainerOptions{Container: id, Repository: "tsuru/python"}
	image, err := cluster.CommitContainer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if image.ID != "596069db4bf5" {
		t.Errorf("CommitContainer: the image container is %s, expected: '596069db4bf5'", image.ID)
	}
	if called {
		t.Errorf("CommitContainer(%q): should not call the all node servers.", id)
	}
	if node := storage.iMap["tsuru/python"]; node != "handler1" {
		t.Errorf("CommitContainer(%q): wrong image ID in the storage. Want %q. Got %q", id, "handler1", node)
	}
}

func TestCommitContainerWithStorageAndImageID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"Id":"596069db4bf5"}`))
	}))
	defer server.Close()
	id := "abc123"
	storage := mapStorage{
		cMap: map[string]string{id: "handler0"},
		iMap: map[string]string{},
	}
	cluster, err := New(nil, &storage, Node{ID: "handler0", Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	opts := dclient.CommitContainerOptions{Container: id}
	image, err := cluster.CommitContainer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if node := storage.iMap[image.ID]; node != "handler0" {
		t.Errorf("CommitContainer(%q): wrong image ID in the storage. Want %q. Got %q", id, "handler0", node)
	}
}

func TestCommitContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	opts := dclient.CommitContainerOptions{Container: id}
	_, err = cluster.CommitContainer(opts)
	expected := &dclient.NoSuchContainer{ID: id}
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("CommitContainer(%q): wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestExportContainer(t *testing.T) {
	content := "tar content of container"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(content))
	}))
	defer server.Close()
	containerID := "3e2f21a89f"
	storage := &mapStorage{cMap: map[string]string{containerID: "handler0"}}
	cluster, err := New(nil, storage, Node{ID: "handler0", Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	out := &bytes.Buffer{}
	err = cluster.ExportContainer(containerID, out)
	if err != nil {
		t.Errorf("ExportContainer: unexpected error: %#v", err.Error())
	}
	if out.String() != content {
		t.Errorf("ExportContainer: wrong out. Want %#v. Got %#v.", content, out.String())
	}
}

func TestExportContainerNotFoundWithStorage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(""))
	}))
	defer server.Close()
	cluster, err := New(nil, &mapStorage{}, Node{ID: "handler0", Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	containerID := "3e2f21a89f"
	out := &bytes.Buffer{}
	err = cluster.ExportContainer(containerID, out)
	if err == nil {
		t.Errorf("ExportContainer: expected error not to be <nil>", err.Error())
	}
}

func TestExportContainerNoStorage(t *testing.T) {
	content := "tar content of container"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(content))
	}))
	defer server.Close()
	cluster, err := New(nil, nil, Node{ID: "handler0", Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	containerID := "3e2f21a89f"
	out := &bytes.Buffer{}
	err = cluster.ExportContainer(containerID, out)
	if err == nil {
		t.Errorf("ExportContainer: expected error not to be <nil>", err.Error())
	}
}

func TestGetNode(t *testing.T) {
	var storage mapStorage
	storage.StoreContainer("e90301", "handler1")
	storage.StoreContainer("e90304", "handler1")
	storage.StoreContainer("e90303", "handler2")
	storage.StoreContainer("e90302", "handler3")
	cluster, err := New(nil, &storage,
		Node{ID: "handler0", Address: "http://localhost:4243"},
		Node{ID: "handler1", Address: "http://localhost:4242"},
		Node{ID: "handler2", Address: "http://localhost:4241"},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cluster.getNodeForContainer("e90302")
	if err != ErrUnknownNode {
		t.Errorf("cluster.getNode(%q): wrong error. Want %#v. Got %#v.", "e90302", ErrUnknownNode, err)
	}
	node, err := cluster.getNodeForContainer("e90301")
	if err != nil {
		t.Error(err)
	}
	if node.id != "handler1" {
		t.Errorf("cluster.getNode(%q): wrong node. Want %q. Got %q.", "e90301", "handler1", node.id)
	}
	_, err = cluster.getNodeForContainer("e90305")
	expected := dclient.NoSuchContainer{ID: "e90305"}
	if !reflect.DeepEqual(err, &expected) {
		t.Errorf("cluster.getNode(%q): wrong error. Want %#v. Got %#v.", "e90305", expected, err)
	}
	cluster, err = New(failingScheduler{}, &storage)
	if err != nil {
		t.Fatal(err)
	}
	_, err = cluster.getNodeForContainer("e90301")
	expectedMsg := "Cannot retrieve list of nodes"
	if err.Error() != expectedMsg {
		t.Errorf("cluster.getNode(%q): wrong error. Want %q. Got %q.", "e90301", expectedMsg, err.Error())
	}
}
