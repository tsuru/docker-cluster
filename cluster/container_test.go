// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateContainer(t *testing.T) {
	body := `{"Id":"e90302"}`
	handler := []bool{false, false}
	server1 := httptest.NewServer(http.HandlerFunc(func (w http.ResponseWriter, r *http.Request) {
		handler[0] = true
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func (w http.ResponseWriter, r *http.Request) {
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
	container, err := cluster.CreateContainer(&config)
	if err != nil {
		t.Fatal(err)
	}
	if container.ID != "e90302" {
		t.Errorf("CreateContainer: wrong container ID. Want %q. Got %q.", "e90302", container.ID)
	}
}
