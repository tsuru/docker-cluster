// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/fsouza/go-dockerclient"
	"testing"
)

func TestRoundRobinSchedule(t *testing.T) {
	c, err := New(&roundRobin{}, &mapStorage{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}
	c.Register("url1", nil)
	c.Register("url2", nil)
	opts := docker.CreateContainerOptions{Config: &docker.Config{}}
	node, err := c.scheduler.Schedule(c, opts, nil)
	if err != nil {
		t.Error(err)
	}
	if node.Address != "url1" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "url1", node.Address)
	}
	node, _ = c.scheduler.Schedule(c, opts, nil)
	if node.Address != "url2" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "url2", node.Address)
	}
	node, _ = c.scheduler.Schedule(c, opts, nil)
	if node.Address != "url1" {
		t.Errorf("roundRobin.Schedule(): wrong node ID. Want %q. Got %q.", "url1", node.Address)
	}
}

func TestScheduleEmpty(t *testing.T) {
	defer func() {
		expected := "No nodes available"
		r := recover().(string)
		if r != expected {
			t.Fatalf("Schedule(): wrong panic message. Want %q. Got %q.", expected, r)
		}
	}()
	c, err := New(&roundRobin{}, &mapStorage{})
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}
	opts := docker.CreateContainerOptions{Config: &docker.Config{}}
	c.scheduler.Schedule(c, opts, nil)
}
