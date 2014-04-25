// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"github.com/fsouza/go-dockerclient"
	"sync"
	"sync/atomic"
)

// Arbitrary options to be sent to the scheduler. This options will
// be only read and interpreted by the scheduler itself.
type SchedulerOptions interface{}

// Scheduler represents a scheduling strategy, that will be used when creating
// new containers.
type Scheduler interface {
	// Schedule creates a new container, returning the ID of the node where
	// the container is running, and the container, or an error.
	Schedule(opts docker.CreateContainerOptions, schedulerOpts SchedulerOptions) (Node, error)

	// Nodes returns a slice of nodes in the scheduler.
	Nodes() ([]Node, error)

	// NodesForOptions returns a slice of nodes that could be used for
	// given options.
	NodesForOptions(schedulerOpts SchedulerOptions) ([]Node, error)
}

// Registrable represents a scheduler that can get new nodes via the Register
// method.
type Registrable interface {
	// Register adds new nodes to the scheduler.
	Register(params map[string]string) error
	// Unregister removes a node from the scheduler.
	Unregister(params map[string]string) error
}

type node struct {
	*docker.Client
	id  string
	edp string
}

type roundRobin struct {
	lastUsed int64
	mut      sync.RWMutex
	stor     Storage
}

func (s *roundRobin) Schedule(opts docker.CreateContainerOptions, schedulerOpts SchedulerOptions) (Node, error) {
	return s.next(), nil
}

func (s *roundRobin) next() Node {
	s.mut.RLock()
	defer s.mut.RUnlock()
	nodes, _ := s.Nodes()
	if len(nodes) == 0 {
		panic("No nodes available")
	}
	index := atomic.AddInt64(&s.lastUsed, 1) % int64(len(nodes))
	return nodes[index]
}

func (s *roundRobin) Register(params map[string]string) error {
	nodes, _ := s.Nodes()
	s.mut.Lock()
	defer s.mut.Unlock()
	if len(nodes) == 0 {
		s.lastUsed = -1
	}
	if s.stor == nil {
		return ErrImmutableCluster
	}
	if params["address"] == "" {
		return errors.New("Invalid address")
	}
	return s.stor.StoreNode(Node{ID: params["ID"], Address: params["address"]})
}

func (s *roundRobin) Unregister(params map[string]string) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.stor == nil {
		return ErrImmutableCluster
	}
	return s.stor.RemoveNode(params["ID"])
}

func (s *roundRobin) Nodes() ([]Node, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if s.stor == nil {
		return nil, ErrImmutableCluster
	}
	return s.stor.RetrieveNodes()
}

func (s *roundRobin) NodesForOptions(schedulerOpts SchedulerOptions) ([]Node, error) {
	return s.Nodes()
}
