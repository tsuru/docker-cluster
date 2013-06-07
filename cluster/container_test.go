// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	dclient "github.com/fsouza/go-dockerclient"
	"net/http"
	"net/http/httptest"
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
