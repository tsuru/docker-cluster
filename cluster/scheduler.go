// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"github.com/fsouza/go-dockerclient"
	"sync"
	"sync/atomic"
)

// Scheduler represents a scheduling strategy, that will be used when creating
// new containers.
type Scheduler interface {
	// Schedule creates a new container, returning the ID of the node where
	// the container is running, and the container, or an error.
	Schedule(opts docker.CreateContainerOptions, config *docker.Config) (string, *docker.Container, error)

	// Nodes returns a slice of nodes in the scheduler.
	Nodes() ([]Node, error)
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
	nodes    []node
	lastUsed int64
	mut      sync.RWMutex
	stor     Storage
}

func (s *roundRobin) Schedule(opts docker.CreateContainerOptions, config *docker.Config) (string, *docker.Container, error) {
	node := s.next()
	container, err := node.CreateContainer(opts, config)
	return node.id, container, err
}

func (s *roundRobin) next() node {
	s.mut.RLock()
	defer s.mut.RUnlock()
	nodes, _ := s.Nodes()
	if len(nodes) == 0 {
		panic("No nodes available")
	}
	index := atomic.AddInt64(&s.lastUsed, 1) % int64(len(nodes))
	cli, err := docker.NewClient(nodes[index].Address)
	if err != nil {
		panic(err)
	}
	return node{Client: cli, edp: nodes[index].Address, id: nodes[index].ID}
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
	return s.stor.StoreNode(params["ID"], params["address"])
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
	result, err := s.stor.RetrieveNodes()
	if err != nil {
		return nil, err
	}
	nodes := []Node{}
	for k, v := range result {
		nodes = append(nodes, Node{ID: k, Address: v})
	}
	return nodes, nil
}
