// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cluster provides types and functions for management of Docker
// clusters, scheduling container operations among hosts running Docker
// (nodes).
package cluster

import (
	"errors"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/fsouza/go-dockerclient"
	"github.com/tsuru/docker-cluster/log"
)

var (
	errStorageMandatory = errors.New("Storage parameter is mandatory")
	errHealerInProgress = errors.New("Healer already running")
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
	RetrieveImage(image string) (host []string, err error)
	RemoveImage(image string) error
}

type NodeStorage interface {
	StoreNode(node Node) error
	RetrieveNodesByMetadata(metadata map[string]string) ([]Node, error)
	RetrieveNodes() ([]Node, error)
	RetrieveNode(address string) (Node, error)
	UpdateNode(node Node) error
	RemoveNode(address string) error
	LockNodeForHealing(address string, isFailure bool) (bool, error)
}

type Storage interface {
	ContainerStorage
	ImageStorage
	NodeStorage
}

// Cluster is the basic type of the package. It manages internal nodes, and
// provide methods for interaction with those nodes, like CreateContainer,
// which creates a container in one node of the cluster.
type Cluster struct {
	scheduler      Scheduler
	stor           Storage
	healer         Healer
	monitoringDone chan bool
}

// New creates a new Cluster, initially composed by the given nodes.
//
// The scheduler parameter defines the scheduling strategy. It defaults
// to round robin if nil.
// The storage parameter is the storage the cluster instance will use.
func New(scheduler Scheduler, storage Storage, nodes ...Node) (*Cluster, error) {
	var (
		c   Cluster
		err error
	)
	if storage == nil {
		return nil, errStorageMandatory
	}
	c.stor = storage
	c.scheduler = scheduler
	c.healer = DefaultHealer{}
	if scheduler == nil {
		c.scheduler = &roundRobin{lastUsed: -1}
	}
	if len(nodes) > 0 {
		for _, n := range nodes {
			err = c.Register(n.Address, n.Metadata)
			if err != nil {
				return &c, err
			}
		}
	}
	return &c, err
}

func (c *Cluster) SetHealer(healer Healer) {
	c.healer = healer
}

// Register adds new nodes to the cluster.
func (c *Cluster) Register(address string, metadata map[string]string) error {
	if address == "" {
		return errors.New("Invalid address")
	}
	node := Node{
		Address:  address,
		Metadata: metadata,
	}
	return c.storage().StoreNode(node)
}

// Unregister removes nodes from the cluster.
func (c *Cluster) Unregister(address string) error {
	return c.storage().RemoveNode(address)
}

func (c *Cluster) UnfilteredNodes() ([]Node, error) {
	return c.storage().RetrieveNodes()
}

func (c *Cluster) Nodes() ([]Node, error) {
	nodes, err := c.storage().RetrieveNodes()
	if err != nil {
		return nil, err
	}
	return NodeList(nodes).filterDisabled(), nil
}

func (c *Cluster) NodesForMetadata(metadata map[string]string) ([]Node, error) {
	nodes, err := c.storage().RetrieveNodesByMetadata(metadata)
	if err != nil {
		return nil, err
	}
	return NodeList(nodes).filterDisabled(), nil
}

func (c *Cluster) StartActiveMonitoring(updateInterval time.Duration) {
	c.monitoringDone = make(chan bool)
	go c.runActiveMonitoring(updateInterval)
}

func (c *Cluster) StopActiveMonitoring() {
	if c.monitoringDone != nil {
		c.monitoringDone <- true
	}
}

func (c *Cluster) WaitAndRegister(address string, metadata map[string]string, timeout time.Duration) error {
	client, err := c.getNodeByAddr(address)
	if err != nil {
		return err
	}
	doneChan := make(chan bool)
	go func() {
		for {
			err = client.Ping()
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		close(doneChan)
	}()
	select {
	case <-doneChan:
	case <-time.After(timeout):
		return errors.New("timed out waiting for node to be ready")
	}
	return c.Register(address, metadata)
}

func (c *Cluster) runActiveMonitoring(updateInterval time.Duration) {
	for {
		var nodes []Node
		var err error
		nodes, err = c.UnfilteredNodes()
		if err != nil {
			log.Errorf("[active-monitoring]: error in UnfilteredNodes: %s", err.Error())
		}
		for _, node := range nodes {
			client, err := c.getNodeByAddr(node.Address)
			if err != nil {
				log.Errorf("[active-monitoring]: error creating client: %s", err.Error())
				continue
			}
			log.Debugf("[active-monitoring]: pinging host: %s", node.Address)
			err = client.Ping()
			if err == nil {
				log.Debugf("[active-monitoring]: Ping OK: %s", node.Address)
				c.handleNodeSuccess(node.Address)
			} else {
				log.Errorf("[active-monitoring]: error in ping: %s", err.Error())
				c.handleNodeError(node.Address, err)
			}
		}
		select {
		case <-c.monitoringDone:
			return
		case <-time.After(updateInterval):
		}
	}
}

func (c *Cluster) handleNodeError(addr string, lastErr error) error {
	locked, err := c.storage().LockNodeForHealing(addr, true)
	if err != nil {
		return err
	}
	if !locked {
		return errHealerInProgress
	}
	go func() {
		node, err := c.storage().RetrieveNode(addr)
		if err != nil {
			return
		}
		node.Healing = HealingData{}
		defer c.storage().UpdateNode(node)
		node.updateError(lastErr)
		duration := c.healer.HandleError(node)
		if duration > 0 {
			node.updateDisabled(time.Now().Add(duration))
		}
	}()
	return nil
}

func (c *Cluster) handleNodeSuccess(addr string) error {
	locked, err := c.storage().LockNodeForHealing(addr, false)
	if err != nil {
		return err
	}
	if !locked {
		return errHealerInProgress
	}
	node, err := c.storage().RetrieveNode(addr)
	if err != nil {
		return err
	}
	node.Healing = HealingData{}
	defer c.storage().UpdateNode(node)
	node.updateSuccess()
	return nil
}

func (c *Cluster) storage() Storage {
	return c.stor
}

type nodeFunc func(node) (interface{}, error)

func (c *Cluster) runOnNodes(fn nodeFunc, errNotFound error, wait bool, nodeAddresses ...string) (interface{}, error) {
	if len(nodeAddresses) == 0 {
		nodes, err := c.Nodes()
		if err != nil {
			return nil, err
		}
		nodeAddresses = make([]string, len(nodes))
		for i, node := range nodes {
			nodeAddresses[i] = node.Address
		}
	}
	var wg sync.WaitGroup
	finish := make(chan int8, len(nodeAddresses))
	errChan := make(chan error, len(nodeAddresses))
	result := make(chan interface{}, len(nodeAddresses))
	for _, addr := range nodeAddresses {
		wg.Add(1)
		client, err := docker.NewClient(addr)
		if err != nil {
			return nil, err
		}
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
		}(node{addr: addr, Client: client})
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
	address, err := retrieveFn(storage)
	if err != nil {
		return n, err
	}
	return c.getNodeByAddr(address)
}

func (c *Cluster) getNodeByAddr(address string) (node, error) {
	var n node
	client, err := docker.NewClient(address)
	if err != nil {
		return n, err
	}
	return node{addr: address, Client: client}, nil
}
