// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/fsouza/go-dockerclient"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestRoundRobinSchedule(t *testing.T) {
	var pulls int32
	body := `{"Id":"e90302"}`
	handler := []bool{false, false}
	image := "tsuru/python"
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/images/create" {
			atomic.AddInt32(&pulls, 1)
			if got := r.URL.Query().Get("fromImage"); got != image {
				t.Errorf("Schedule: wrong image name. Want %q. Got %q.", image, got)
			}
		}
		handler[0] = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/images/create" {
			atomic.AddInt32(&pulls, 1)
			if got := r.URL.Query().Get("fromImage"); got != image {
				t.Errorf("Schedule: wrong image name. Want %q. Got %q.", image, got)
			}
		}
		handler[1] = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server2.Close()
	scheduler := &roundRobin{stor: &mapStorage{}}
	scheduler.Register(map[string]string{"ID": "node0", "address": server1.URL})
	scheduler.Register(map[string]string{"ID": "node1", "address": server2.URL})
	opts := docker.CreateContainerOptions{Config: &docker.Config{Memory: 67108864, Image: image}}
	id, container, err := scheduler.Schedule(opts)
	if err != nil {
		t.Error(err)
	}
	if id != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", id)
	}
	if container.ID != "e90302" {
		t.Errorf("roundRobin.Schedule(): wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
	id, _, _ = scheduler.Schedule(opts)
	if id != "node1" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node1", id)
	}
	id, _, _ = scheduler.Schedule(opts)
	if id != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", id)
	}
}

func TestNextEmpty(t *testing.T) {
	defer func() {
		expected := "No nodes available"
		r := recover().(string)
		if r != expected {
			t.Fatalf("next(): wrong panic message. Want %q. Got %q.", expected, r)
		}
	}()
	scheduler := &roundRobin{stor: &mapStorage{}}
	scheduler.next()
}

func TestRoundRobinNodes(t *testing.T) {
	nodes := []Node{
		{ID: "server0", Address: "http://localhost:8080"},
		{ID: "server1", Address: "http://localhost:8081"},
	}
	scheduler := &roundRobin{stor: &mapStorage{}}
	for _, n := range nodes {
		scheduler.Register(map[string]string{"address": n.Address, "ID": n.ID})
	}
	got, err := scheduler.Nodes()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(got, nodes) {
		t.Errorf("roundRobin.Nodes(): wrong result. Want %#v. Got %#v.", nodes, got)
	}
}

func TestRoundRobinNodesUnregister(t *testing.T) {
	nodes := []Node{
		{ID: "server0", Address: "http://localhost:8080"},
		{ID: "server1", Address: "http://localhost:8081"},
	}
	scheduler := &roundRobin{stor: &mapStorage{}}
	for _, n := range nodes {
		scheduler.Register(map[string]string{"address": n.Address, "ID": n.ID})
	}
	scheduler.Unregister(map[string]string{"address": nodes[0].Address, "ID": nodes[0].ID})
	got, err := scheduler.Nodes()
	if err != nil {
		t.Error(err)
	}
	expected := []Node{{ID: "server1", Address: "http://localhost:8081"}}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("roundRobin.Nodes(): wrong result. Want %#v. Got %#v.", nodes, got)
	}
}
