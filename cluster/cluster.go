// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cluster provides types and functions for management of Docker
// clusters, scheduling container operations among hosts running Docker
// (nodes).
package cluster

import (
	"errors"
	"github.com/fsouza/go-dockerclient"
	"net/http"
	"reflect"
	"sync"
)

var (
	// ErrUnknownNode is the error returned when an unknown node is stored in the
	// storage. This error means some kind of inconsistence between the storage and
	// the cluster.
	ErrUnknownNode = errors.New("Unknown node")

	// ErrImmutableCluster is the error returned by Register when the cluster is
	// immutable, meaning that no new nodes can be registered.
	ErrImmutableCluster = errors.New("Immutable cluster")

	errStorageDisabled = errors.New("Storage is disabled")
)

// ContainerStorage provides methods to store and retrieve information about
// the relation between the node and the container. It can be easily
// represented as a key-value storage.
//
// The relevant information is: in which host the given container is running?
type ContainerStorage interface {
	StoreContainer(container, host string) error
	RetrieveContainer(container string) (host string, err error)
	RemoveContainer(container string) error
}

// ImageStorage works like ContainerStorage, but stores information about
// images and hosts.
type ImageStorage interface {
	StoreImage(image, host string) error
	RetrieveImage(image string) (host string, err error)
	RemoveImage(image string) error
}

type NodeStorage interface {
	StoreNode(id, address string) error
	RemoveNode(id string) error
}

type Storage interface {
	ContainerStorage
	ImageStorage
	NodeStorage
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

	stor  Storage
	mutex sync.RWMutex
}

// New creates a new Cluster, composed by the given nodes.
//
// The parameter Scheduler defines the scheduling strategy, and cannot change.
// It is optional, when set to nil, the cluster will use a round robin strategy
// defined internaly.
func New(scheduler Scheduler, storage Storage, nodes ...Node) (*Cluster, error) {
	var (
		c   Cluster
		err error
	)
	c.stor = storage
	c.scheduler = scheduler
	if scheduler == nil {
		c.scheduler = &roundRobin{lastUsed: -1}
	}
	if len(nodes) > 0 {
		for _, n := range nodes {
			err = c.Register(map[string]string{"address": n.Address, "ID": n.ID})
			if err != nil {
				return &c, err
			}
		}
	}
	return &c, err
}

// Register adds new nodes to the cluster.
func (c *Cluster) Register(params map[string]string) error {
	if r, ok := c.scheduler.(Registrable); ok {
		return r.Register(params)
	}
	return ErrImmutableCluster
}

// Unregister removes nodes from the cluster.
func (c *Cluster) Unregister(params map[string]string) error {
	if r, ok := c.scheduler.(Registrable); ok {
		return r.Unregister(params)
	}
	return ErrImmutableCluster
}

func (c *Cluster) Nodes() ([]Node, error) {
	return c.scheduler.Nodes()
}

// SetStorage defines the storage in use the cluster.
func (c *Cluster) SetStorage(storage Storage) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stor = storage
}

func (c *Cluster) storage() Storage {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.stor
}

type nodeFunc func(node) (interface{}, error)

func (c *Cluster) runOnNodes(fn nodeFunc, errNotFound error, wait bool) (interface{}, error) {
	nodes, err := c.scheduler.Nodes()
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	finish := make(chan int8, len(nodes))
	errChan := make(chan error, len(nodes))
	result := make(chan interface{}, len(nodes))
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
	if wait {
		wg.Wait()
		select {
		case value := <-result:
			return value, nil
		case err := <-errChan:
			return nil, err
		default:
			return nil, errNotFound
		}
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

func (c *Cluster) getNode(retrieveFn func(Storage) (string, error)) (node, error) {
	var n node
	storage := c.storage()
	if storage == nil {
		return n, errStorageDisabled
	}
	id, err := retrieveFn(storage)
	if err != nil {
		return n, err
	}
	nodes, err := c.scheduler.Nodes()
	if err != nil {
		return n, err
	}
	for _, nd := range nodes {
		if nd.ID == id {
			client, _ := docker.NewClient(nd.Address)
			return node{id: nd.ID, Client: client, edp: nd.Address}, nil
		}
	}
	return n, ErrUnknownNode
}
