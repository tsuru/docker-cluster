// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	dcli "github.com/fsouza/go-dockerclient"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
)

func TestNewCluster(t *testing.T) {
	var tests = []struct {
		scheduler Scheduler
		input     []Node
		fail      bool
	}{
		{
			&roundRobin{},
			[]Node{{ID: "something", Address: "http://localhost:8083"}},
			false,
		},
		{
			&roundRobin{},
			[]Node{{ID: "something", Address: ""}, {ID: "otherthing", Address: "http://localhost:8083"}},
			true,
		},
		{
			nil,
			[]Node{{ID: "something", Address: "http://localhost:8083"}},
			false,
		},
	}
	for _, tt := range tests {
		_, err := New(&roundRobin{}, tt.input...)
		if tt.fail && err == nil || !tt.fail && err != nil {
			t.Errorf("cluster.New() for input %#v. Expect failure: %v. Got: %v.", tt.input, tt.fail, err)
		}
	}
}

func TestRegister(t *testing.T) {
	var scheduler roundRobin
	cluster, err := New(&scheduler)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(Node{ID: "abcdef", Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	node := scheduler.next()
	if node.id != "abcdef" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdef", node.id)
	}
	err = cluster.Register(Node{ID: "abcdefg", Address: "http://localhost:4243"})
	if err != nil {
		t.Fatal(err)
	}
	node = scheduler.next()
	if node.id != "abcdefg" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdefg", node.id)
	}
	node = scheduler.next()
	if node.id != "abcdef" {
		t.Errorf("Register failed. Got wrong ID. Want %q. Got %q.", "abcdef", node.id)
	}
}

func TestRegisterSchedulerUnableToRegister(t *testing.T) {
	var scheduler fakeScheduler
	cluster, err := New(scheduler)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(Node{ID: "abcdef", Address: ""})
	if err != ErrImmutableCluster {
		t.Error(err)
	}
}

func TestRegisterFailure(t *testing.T) {
	cluster, err := New(&roundRobin{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Register(Node{ID: "abcdef", Address: ""})
	if err == nil {
		t.Error("Expected non-nil error, got <nil>.")
	}
}

func TestRunOnNodesStress(t *testing.T) {
	n := 1000
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(16))
	body := `{"Id":"e90302","Path":"date","Args":[]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer server.Close()
	cluster, err := New(nil, Node{ID: "server0", Address: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	id := "e90302"
	for i := 0; i < rand.Intn(10)+n; i++ {
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
}

func TestSetStorage(t *testing.T) {
	var c Cluster
	var storage, other mapStorage
	c.SetStorage(&storage)
	if c.storage() != &storage {
		t.Errorf("Cluster.SetStorage(): did not change the storage")
	}
	c.SetStorage(&other)
	if c.storage() != &other {
		t.Errorf("Cluster.SetStorage(): did not change the storage")
	}
}

type mapStorage struct {
	m map[string]string
	sync.Mutex
}

func (s *mapStorage) Store(containerID, hostID string) error {
	s.Lock()
	defer s.Unlock()
	if s.m == nil {
		s.m = make(map[string]string)
	}
	s.m[containerID] = hostID
	return nil
}

func (s *mapStorage) Retrieve(containerID string) (string, error) {
	s.Lock()
	defer s.Unlock()
	host, ok := s.m[containerID]
	if !ok {
		return "", &dcli.NoSuchContainer{ID: containerID}
	}
	return host, nil
}

type fakeScheduler struct{}

func (fakeScheduler) Schedule(*docker.Config) (string, *docker.Container, error) {
	return "", nil, nil
}

func (fakeScheduler) Nodes() []Node {
	return nil
}
