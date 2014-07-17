// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"bytes"
	"fmt"
	"github.com/fsouza/go-dockerclient"
	cstorage "github.com/tsuru/docker-cluster/storage"
	"github.com/tsuru/tsuru/safe"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
)

func TestCreateContainer(t *testing.T) {
	body := `{"Id":"e90302"}`
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864, Image: "myimg"}
	nodeAddr, container, err := cluster.CreateContainer(docker.CreateContainerOptions{Config: &config})
	if err != nil {
		t.Fatal(err)
	}
	if nodeAddr != server1.URL {
		t.Errorf("CreateContainer: wrong node  ID. Want %q. Got %q.", server1.URL, nodeAddr)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
	imageHosts, err := cluster.storage().RetrieveImage("myimg")
	if err != nil {
		t.Fatal(err)
	}
	if len(imageHosts) != 1 {
		t.Fatal("CreateContainer: should store image in host, none found")
	}
	if imageHosts[0] != server1.URL {
		t.Fatalf("CreateContainer: should store image in host, found %s", imageHosts[0])
	}
}

func TestCreateContainerOptions(t *testing.T) {
	body := `{"Id":"e90302"}`
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864, Image: "myimg"}
	opts := docker.CreateContainerOptions{Name: "name", Config: &config}
	nodeAddr, container, err := cluster.CreateContainer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if nodeAddr != server1.URL {
		t.Errorf("CreateContainer: wrong node  ID. Want %q. Got %q.", server1.URL, nodeAddr)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
}

func TestCreateContainerSchedulerOpts(t *testing.T) {
	body := `{"Id":"e90302"}`
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	scheduler := optsScheduler{roundRobin{lastUsed: -1}}
	cluster, err := New(scheduler, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864, Image: "myimg"}
	opts := docker.CreateContainerOptions{Name: "name", Config: &config}
	schedulerOpts := "myOpt"
	nodeAddr, container, err := cluster.CreateContainerSchedulerOpts(opts, schedulerOpts)
	if err != nil {
		t.Fatal(err)
	}
	if nodeAddr != server1.URL {
		t.Errorf("CreateContainer: wrong node  ID. Want %q. Got %q.", server1.URL, nodeAddr)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
	schedulerOpts = "myOptX"
	nodeAddr, container, err = cluster.CreateContainerSchedulerOpts(opts, schedulerOpts)
	if err == nil || err.Error() != "Invalid option myOptX" {
		t.Fatal("Expected error but none returned.")
	}
}

func TestCreateContainerFailure(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "NoSuchImage", http.StatusNotFound)
	}))
	defer server1.Close()
	cluster, err := New(nil, &MapStorage{}, Node{Address: server1.URL})
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864}
	_, _, err = cluster.CreateContainer(docker.CreateContainerOptions{Config: &config})
	expected := "No nodes available"
	if err == nil || err.Error() != expected {
		t.Errorf("Expected error %q, got: %#v", expected, err)
	}
}

func TestCreateContainerSpecifyNode(t *testing.T) {
	var requests []string
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `{"Id":"e90302"}`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.RequestURI)
		body := `{"Id":"e90303"}`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	var storage MapStorage
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CreateContainerOptions{Config: &docker.Config{
		Memory: 67108864,
		Image:  "myImage",
	}}
	nodeAddr, container, err := cluster.CreateContainer(opts, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	if nodeAddr != server2.URL {
		t.Errorf("CreateContainer: wrong node ID. Want %q. Got %q.", server2.URL, nodeAddr)
	}
	if container.ID != "e90303" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90303", container.ID)
	}
	host, _ := storage.RetrieveContainer("e90303")
	if host != server2.URL {
		t.Errorf("Cluster.CreateContainer() with storage: wrong data. Want %#v. Got %#v.", server2.URL, host)
	}
	if len(requests) != 2 {
		t.Errorf("Expected 2 api calls, got %d.", len(requests))
	}
	expectedReq := "/images/create?fromImage=myImage"
	if requests[0] != expectedReq {
		t.Errorf("Incorrect request 0. Want %#v. Got %#v", expectedReq, requests[0])
	}
	expectedReq = "/containers/create"
	if requests[1] != expectedReq {
		t.Errorf("Incorrect request 1. Want %#v. Got %#v", expectedReq, requests[1])
	}
}

func TestCreateContainerSpecifyUnknownNode(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := `{"Id":"e90302"}`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CreateContainerOptions{Config: &docker.Config{Memory: 67108864}}
	nodeAddr, container, err := cluster.CreateContainer(opts, "invalid.addr")
	if nodeAddr != "invalid.addr" {
		t.Errorf("Got wrong node ID. Want %q. Got %q.", "invalid.addr", nodeAddr)
	}
	if container != nil {
		t.Errorf("Got unexpected value for container. Want <nil>. Got %#v", container)
	}
}

func TestCreateContainerWithStorage(t *testing.T) {
	body := `{"Id":"e90302"}`
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	var storage MapStorage
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864, Image: "myimg"}
	_, _, err = cluster.CreateContainer(docker.CreateContainerOptions{Config: &config})
	if err != nil {
		t.Fatal(err)
	}
	host, _ := storage.RetrieveContainer("e90302")
	if host != server1.URL {
		t.Errorf("Cluster.CreateContainer() with storage: wrong data. Want %#v. Got %#v.", server1.URL, host)
	}
}

type firstNodeScheduler struct{}

func (firstNodeScheduler) Schedule(c *Cluster, opts docker.CreateContainerOptions, schedulerOpts SchedulerOptions) (Node, error) {
	var node Node
	nodes, err := c.Nodes()
	if err != nil {
		return node, err
	}
	if len(nodes) == 0 {
		return node, fmt.Errorf("no nodes in scheduler")
	}
	return nodes[0], nil
}

func TestCreateContainerTryAnotherNodeInFailure(t *testing.T) {
	body := `{"Id":"e90302"}`
	called1 := false
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called1 = true
		http.Error(w, "NoSuchImage", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	cluster, err := New(firstNodeScheduler{}, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := docker.Config{Memory: 67108864, Image: "myimg"}
	nodeAddr, container, err := cluster.CreateContainer(docker.CreateContainerOptions{Config: &config})
	if err != nil {
		t.Fatal(err)
	}
	if called1 != true {
		t.Error("CreateContainer: server1 should've been called.")
	}
	if nodeAddr != server2.URL {
		t.Errorf("CreateContainer: wrong node  ID. Want %q. Got %q.", server2.URL, nodeAddr)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
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
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	id := "e90302"
	container, err := cluster.InspectContainer(id)
	if container != nil {
		t.Errorf("InspectContainer(%q): Expected <nil> container, got %#v.", id, container)
	}
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("InspectContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestInspectContainerNoSuchContainerWithStorage(t *testing.T) {
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	id := "e90302"
	container, err := cluster.InspectContainer(id)
	if container != nil {
		t.Errorf("InspectContainer(%q): Expected <nil> container, got %#v.", id, container)
	}
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("InspectContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestInspectContainerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such container", http.StatusInternalServerError)
	}))
	defer server.Close()
	cluster, err := New(nil, &MapStorage{}, Node{Address: server.URL})
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
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.KillContainer(docker.KillContainerOptions{ID: id})
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.KillContainer(docker.KillContainerOptions{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("KillContainer(%q): should not call the node server", id)
	}
}

func TestKillContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.KillContainer(docker.KillContainerOptions{ID: id})
	expected := cstorage.ErrNoSuchContainer
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
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
		Node{Address: server3.URL},
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
	containers, err := cluster.ListContainers(docker.ListContainersOptions{})
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
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := []docker.APIContainers{
		{ID: "8dfafdbc3a40", Image: "base:latest", Command: "echo 1", Created: 1367854155, Status: "Exit 0"},
	}
	containers, err := cluster.ListContainers(docker.ListContainersOptions{})
	if err == nil {
		t.Error("ListContainers: Expected non-nil error, got <nil>")
	}
	if !reflect.DeepEqual(containers, expected) {
		t.Errorf("ListContainers: Want %#v. Got %#v.", expected, containers)
	}
}

func TestListContainersSchedulerFailure(t *testing.T) {
	cluster, err := New(nil, &failingStorage{})
	if err != nil {
		t.Fatal(err)
	}
	containers, err := cluster.ListContainers(docker.ListContainersOptions{})
	expected := "storage error"
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
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RemoveContainer(docker.RemoveContainerOptions{ID: id})
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RemoveContainer(docker.RemoveContainerOptions{ID: id})
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.RemoveContainer(docker.RemoveContainerOptions{ID: id})
	expected := cstorage.ErrNoSuchContainer
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
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.StartContainer(id, nil)
	expected := cstorage.ErrNoSuchContainer
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
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.StopContainer(id, 10)
	expected := cstorage.ErrNoSuchContainer
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
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.RestartContainer(id, 10)
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("RestartContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestPauseContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/pause" {
			http.Error(w, "No such container", http.StatusNotFound)
		}
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/pause" {
			called = true
			w.Write([]byte("ok"))
		}
	}))
	defer server2.Close()
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.PauseContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("PauseContainer(%q): Did not call node HTTP server", id)
	}
}

func TestPauseContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/pause" {
			called = true
			http.Error(w, "No such container", http.StatusNotFound)
		}
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/pause" {
			w.Write([]byte("ok"))
		}
	}))
	defer server2.Close()
	id := "abc123"
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.PauseContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("PauseContainer(%q): should not call the node server", id)
	}
}

func TestPauseContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.PauseContainer(id)
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("PauseContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestUnpauseContainer(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/unpause" {
			http.Error(w, "No such container", http.StatusNotFound)
		}
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/unpause" {
			called = true
			w.Write([]byte("ok"))
		}
	}))
	defer server2.Close()
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.UnpauseContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("UnpauseContainer(%q): Did not call node HTTP server", id)
	}
}

func TestUnpauseContainerWithStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/unpause" {
			called = true
			http.Error(w, "No such container", http.StatusNotFound)
		}
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/containers/abc123/unpause" {
			w.Write([]byte("ok"))
		}
	}))
	defer server2.Close()
	id := "abc123"
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.UnpauseContainer(id)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Errorf("UnpauseContainer(%q): should not call the node server", id)
	}
}

func TestUnpauseContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	err = cluster.UnpauseContainer(id)
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("UnpauseContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	id := "abc123"
	storage := &MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
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
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	expectedStatus := -1
	status, err := cluster.WaitContainer(id)
	if status != expectedStatus {
		t.Errorf("WaitContainer(%q): wrong status. Want %d. Got %d.", id, expectedStatus, status)
	}
	expected := cstorage.ErrNoSuchContainer
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
		w.Write([]byte{1, 0, 0, 0, 0, 0, 0, 18})
		w.Write([]byte("something happened"))
	}))
	defer server2.Close()
	storage := &MapStorage{}
	err := storage.StoreContainer("abcdef", server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.AttachToContainerOptions{
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.AttachToContainerOptions{
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abcdef"
	opts := docker.AttachToContainerOptions{
		Container:    "abcdef",
		OutputStream: &safe.Buffer{},
		Logs:         true,
		Stdout:       true,
	}
	err = cluster.AttachToContainer(opts)
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("AttachToContainer(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
	}
}

func TestLogs(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "container not found", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.Write([]byte{1, 0, 0, 0, 0, 0, 0, 18})
		w.Write([]byte("something happened"))
	}))
	defer server2.Close()
	storage := &MapStorage{}
	err := storage.StoreContainer("abcdef", server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.LogsOptions{
		Container:    "abcdef",
		OutputStream: &safe.Buffer{},
		Stdout:       true,
		Stderr:       true,
	}
	err = cluster.Logs(opts)
	if err != nil {
		t.Errorf("Logs: unexpected error. Want <nil>. Got %#v.", err)
	}
	if !called {
		t.Error("Logs: Did not call the remote HTTP API")
	}
}

func TestLogsWithStorage(t *testing.T) {
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.LogsOptions{
		Container:    id,
		OutputStream: &safe.Buffer{},
		Stdout:       true,
		Stderr:       true,
	}
	err = cluster.Logs(opts)
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("Logs(): should not call the node server")
	}
}

func TestLogsContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:8282"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abcdef"
	opts := docker.LogsOptions{
		Container:    "abcdef",
		OutputStream: &safe.Buffer{},
		Stdout:       true,
		Stderr:       true,
	}
	err = cluster.Logs(opts)
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("Logs(%q): Wrong error. Want %#v. Got %#v.", id, expected, err)
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
	storage := &MapStorage{}
	err := storage.StoreContainer("abcdef", server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CommitContainerOptions{
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
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CommitContainerOptions{
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
	storage := MapStorage{}
	err := storage.StoreContainer(id, server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CommitContainerOptions{Container: id, Repository: "tsuru/python"}
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
	nodes, _ := storage.RetrieveImage("tsuru/python")
	if !reflect.DeepEqual(nodes, []string{server2.URL}) {
		t.Errorf("CommitContainer(%q): wrong image ID in the storage. Want %q. Got %q", id, []string{server2.URL}, nodes)
	}
}

func TestCommitContainerWithStorageAndImageID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"Id":"596069db4bf5"}`))
	}))
	defer server.Close()
	id := "abc123"
	storage := MapStorage{}
	err := storage.StoreContainer(id, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &storage, Node{Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.CommitContainerOptions{Container: id}
	image, err := cluster.CommitContainer(opts)
	if err != nil {
		t.Fatal(err)
	}
	nodes, _ := storage.RetrieveImage(image.ID)
	if !reflect.DeepEqual(nodes, []string{server.URL}) {
		t.Errorf("CommitContainer(%q): wrong image ID in the storage. Want %q. Got %q", id, []string{server.URL}, nodes)
	}
}

func TestCommitContainerNotFoundWithStorage(t *testing.T) {
	cluster, err := New(nil, &MapStorage{}, Node{Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	id := "abc123"
	opts := docker.CommitContainerOptions{Container: id}
	_, err = cluster.CommitContainer(opts)
	expected := cstorage.ErrNoSuchContainer
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
	storage := &MapStorage{}
	err := storage.StoreContainer(containerID, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, storage, Node{Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	out := &bytes.Buffer{}
	err = cluster.ExportContainer(docker.ExportContainerOptions{ID: containerID, OutputStream: out})
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	containerID := "3e2f21a89f"
	out := &bytes.Buffer{}
	err = cluster.ExportContainer(docker.ExportContainerOptions{ID: containerID, OutputStream: out})
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
	cluster, err := New(nil, &MapStorage{}, Node{Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	containerID := "3e2f21a89f"
	out := &bytes.Buffer{}
	err = cluster.ExportContainer(docker.ExportContainerOptions{ID: containerID, OutputStream: out})
	if err == nil {
		t.Errorf("ExportContainer: expected error not to be <nil>", err.Error())
	}
}

func TestGetNode(t *testing.T) {
	var storage MapStorage
	storage.StoreContainer("e90301", "http://localhost:4242")
	storage.StoreContainer("e90304", "http://localhost:4242")
	storage.StoreContainer("e90303", "http://localhost:4241")
	storage.StoreContainer("e90302", "http://another")
	cluster, err := New(nil, &storage,
		Node{Address: "http://localhost:4243"},
		Node{Address: "http://localhost:4242"},
		Node{Address: "http://localhost:4241"},
	)
	if err != nil {
		t.Fatal(err)
	}
	node, err := cluster.getNodeForContainer("e90302")
	if err != nil {
		t.Error(err)
	}
	if node.addr != "http://another" {
		t.Errorf("cluster.getNode(%q): wrong node. Want %q. Got %q.", "e90302", "http://another", node.addr)
	}
	node, err = cluster.getNodeForContainer("e90301")
	if err != nil {
		t.Error(err)
	}
	if node.addr != "http://localhost:4242" {
		t.Errorf("cluster.getNode(%q): wrong node. Want %q. Got %q.", "e90301", "http://localhost:4242", node.addr)
	}
	_, err = cluster.getNodeForContainer("e90305")
	expected := cstorage.ErrNoSuchContainer
	if !reflect.DeepEqual(err, expected) {
		t.Errorf("cluster.getNode(%q): wrong error. Want %#v. Got %#v.", "e90305", expected, err)
	}
	cluster, err = New(nil, failingStorage{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = cluster.getNodeForContainer("e90301")
	expectedMsg := "storage error"
	if err.Error() != expectedMsg {
		t.Errorf("cluster.getNode(%q): wrong error. Want %q. Got %q.", "e90301", expectedMsg, err.Error())
	}
}
