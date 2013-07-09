// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cluster provides types and functions for management of Docker
// clusters, scheduling container operations among hosts running Docker
// (nodes).
package cluster

import (
	"github.com/fsouza/go-dockerclient"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
)

// Node represents a host running Docker. Each node has an ID and an address
// (in the form <scheme>://<host>:<port>/).
type Node struct {
	ID      string
	Address string
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
	if len(c.nodes) == 0 {
		panic("No nodes available")
	}
	index := atomic.AddInt64(&c.lastUsed, 1) % int64(len(c.nodes))
	return c.nodes[index]
}

// Register adds new nodes to the cluster.
func (c *Cluster) Register(nodes ...Node) error {
	for _, n := range nodes {
		client, err := docker.NewClient(n.Address)
		if err != nil {
			return err
		}
		c.mut.Lock()
		c.nodes = append(c.nodes, node{id: n.ID, Client: client})
		c.mut.Unlock()
	}
	return nil
}

type nodeFunc func(node) (interface{}, error)

func (c *Cluster) runOnNodes(fn nodeFunc, errNotFound error) (interface{}, error) {
	c.mut.RLock()
	defer c.mut.RUnlock()
	var wg sync.WaitGroup
	finish := make(chan int8, 1)
	errChan := make(chan error, len(c.nodes))
	result := make(chan interface{}, 1)
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n node) {
			defer wg.Done()
			value, err := fn(n)
			if err == nil {
				result <- value
			} else if e, ok := err.(*docker.Error); ok && e.Status == http.StatusNotFound {
				return
			} else if !reflect.DeepEqual(err, errNotFound) {
				errChan <- err
			}
		}(n)
	}
	go func() {
		wg.Wait()
		close(finish)
	}()
	select {
	case value := <-result:
		return value, nil
	case err := <-errChan:
		return nil, err
	case <-finish:
		select {
		case value := <-result:
			return value, nil
		default:
			return nil, errNotFound
		}
	}
}
