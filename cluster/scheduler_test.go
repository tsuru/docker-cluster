// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRoundRobinSchedule(t *testing.T) {
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
	var scheduler roundRobin
	scheduler.Register(
		Node{ID: "node0", Address: server1.URL},
		Node{ID: "node1", Address: server2.URL},
	)
	id, container, err := scheduler.Schedule(&docker.Config{Memory: 67108864})
	if err != nil {
		t.Error(err)
	}
	if id != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", id)
	}
	if container.ID != "e90302" {
		t.Errorf("roundRobin.Schedule(): wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
	id, _, _ = scheduler.Schedule(&docker.Config{Memory: 67108864})
	if id != "node1" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node1", id)
	}
	id, _, _ = scheduler.Schedule(&docker.Config{Memory: 67108864})
	if id != "node0" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "node0", id)
	}
}
