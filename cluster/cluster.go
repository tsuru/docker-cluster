// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cluster provides types and functions for management of Docker
// clusters, scheduling container operations among hosts running Docker
// (nodes).
package cluster

import (
	"github.com/fsouza/go-dockerclient"
	"sync"
	"sync/atomic"
)

// Node represents a host running Docker. Each node has an ID and an address
// (in the form <scheme>://<host>:<port>/).
type Node struct {
	ID      string
	Address string
}

type node struct {
	*docker.Client
	id   string
	load int64
}

// Cluster is the basic type of the package. It manages internal nodes, and
// provide methods for interaction with those nodes, like CreateContainer,
// which creates a container in one node of the cluster.
type Cluster struct {
	nodes    []node
	lastUsed int64
	mut      sync.RWMutex
}

// New creates a new Cluster, composed of the given nodes.
func New(nodes ...Node) (*Cluster, error) {
	c := Cluster{
		nodes:    make([]node, len(nodes)),
		lastUsed: -1,
	}
	for i, n := range nodes {
		client, err := docker.NewClient(n.Address)
		if err != nil {
			return nil, err
		}
		c.nodes[i] = node{
			id:     n.ID,
			Client: client,
		}
	}
	return &c, nil
}

func (c *Cluster) next() node {
	c.mut.RLock()
	defer c.mut.RUnlock()
	index := atomic.AddInt64(&c.lastUsed, 1) % int64(len(c.nodes))
	return c.nodes[index]
}

// Register adds new nodes to the cluster.
func (c *Cluster) Register(nodes ...Node) error {
	c.mut.Lock()
	defer c.mut.Unlock()
	for _, n := range nodes {
		client, err := docker.NewClient(n.Address)
		if err != nil {
			return err
		}
		c.nodes = append(c.nodes, node{id: n.ID, Client: client})
	}
	return nil
}
