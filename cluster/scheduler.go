// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	dcli "github.com/fsouza/go-dockerclient"
	"sync"
	"sync/atomic"
)

// Scheduler represents a scheduling strategy, that will be used when creating
// new containers.
type Scheduler interface {
	// Schedule creates a new container, returning the ID of the node where
	// the container is running, and the container, or an error.
	Schedule(config *docker.Config) (string, *docker.Container, error)

	// Register adds new nodes to the scheduler.
	Register(nodes ...Node) error

	// Nodes returns a slice of nodes in the scheduler.
	Nodes() []Node
}

type node struct {
	*dcli.Client
	id  string
	edp string
}

type roundRobin struct {
	nodes    []node
	lastUsed int64
	mut      sync.RWMutex
}

func (s *roundRobin) Schedule(config *docker.Config) (string, *docker.Container, error) {
	node := s.next()
	container, err := node.CreateContainer(config)
	return node.id, container, err
}

func (s *roundRobin) next() node {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if len(s.nodes) == 0 {
		panic("No nodes available")
	}
	index := atomic.AddInt64(&s.lastUsed, 1) % int64(len(s.nodes))
	return s.nodes[index]
}

func (s *roundRobin) Register(nodes ...Node) error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if len(s.nodes) == 0 {
		s.lastUsed = -1
		s.nodes = make([]node, 0, len(nodes))
	}
	for _, n := range nodes {
		client, err := dcli.NewClient(n.Address)
		if err != nil {
			return err
		}
		s.nodes = append(s.nodes, node{Client: client, edp: n.Address, id: n.ID})
	}
	return nil
}

func (s *roundRobin) Nodes() []Node {
	s.mut.RLock()
	defer s.mut.RUnlock()
	nodes := make([]Node, len(s.nodes))
	for i, node := range s.nodes {
		nodes[i] = Node{ID: node.id, Address: node.edp}
	}
	return nodes
}
