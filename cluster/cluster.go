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
)

// Storage provides methods to store and retrieve information about the
// relation between the node and the container. It can be easily represented as
// a key-value storage.
//
// The relevant information is: in which host the given container is running?
type Storage interface {
	Store(container, host string) error
	Retrieve(container string) (host string, err error)
}

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
	scheduler Scheduler
}

// New creates a new Cluster, composed by the given nodes.
//
// The parameter Scheduler defines the scheduling strategy, and cannot change.
// It is optional, when set to nil, the cluster will use a round robin strategy
// defined internaly.
func New(scheduler Scheduler, nodes ...Node) (*Cluster, error) {
	c := Cluster{}
	c.scheduler = scheduler
	if scheduler == nil {
		c.scheduler = &roundRobin{lastUsed: -1}
	}
	return &c, c.Register(nodes...)
}

// Register adds new nodes to the cluster.
func (c *Cluster) Register(nodes ...Node) error {
	return c.scheduler.Register(nodes...)
}

type nodeFunc func(node) (interface{}, error)

func (c *Cluster) runOnNodes(fn nodeFunc, errNotFound error) (interface{}, error) {
	nodes := c.scheduler.Nodes()
	var wg sync.WaitGroup
	finish := make(chan int8, 1)
	errChan := make(chan error, len(nodes))
	result := make(chan interface{}, 1)
	for _, n := range nodes {
		wg.Add(1)
		client, _ := docker.NewClient(n.Address)
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
		}(node{id: n.ID, Client: client})
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
