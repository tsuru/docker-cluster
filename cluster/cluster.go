// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cluster provides types and functions for management of Docker
// clusters, scheduling container operations among hosts running Docker
// (nodes).
package cluster

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/fsouza/go-dockerclient"
	dtesting "github.com/fsouza/go-dockerclient/testing"
	"github.com/tsuru/docker-cluster/log"
)

var (
	errStorageMandatory = errors.New("Storage parameter is mandatory")
	errHealerInProgress = errors.New("Healer already running")
)

var (
	pingClient       = clientWithTimeout(5*time.Second, 1*time.Minute)
	timeout10Client  = clientWithTimeout(10*time.Second, 1*time.Hour)
	persistentClient = clientWithTimeout(10*time.Second, 0)
)

type node struct {
	*docker.Client
	addr string
}

func (n *node) setPersistentClient() {
	n.HTTPClient = persistentClient
}

// ContainerStorage provides methods to store and retrieve information about
// the relation between the node and the container. It can be easily
// represented as a key-value storage.
//
// The relevant information is: in which host the given container is running?
type ContainerStorage interface {
	StoreContainer(container, host string) error
	RetrieveContainer(container string) (host string, err error)
	RemoveContainer(container string) error
	RetrieveContainers() ([]Container, error)
}

// ImageStorage works like ContainerStorage, but stores information about
// images and hosts.
type ImageStorage interface {
	StoreImage(repo, id, host string) error
	RetrieveImage(repo string) (Image, error)
	RemoveImage(repo, id, host string) error
	RetrieveImages() ([]Image, error)
}

type NodeStorage interface {
	StoreNode(node Node) error
	RetrieveNodesByMetadata(metadata map[string]string) ([]Node, error)
	RetrieveNodes() ([]Node, error)
	RetrieveNode(address string) (Node, error)
	UpdateNode(node Node) error
	RemoveNode(address string) error
	LockNodeForHealing(address string, isFailure bool, timeout time.Duration) (bool, error)
	ExtendNodeLock(address string, timeout time.Duration) error
	UnlockNode(address string) error
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
	dryServer      *dtesting.DockerServer
}

type DockerNodeError struct {
	node node
	cmd  string
	err  error
}

func (n DockerNodeError) Error() string {
	if n.cmd == "" {
		return fmt.Sprintf("error in docker node %q: %s", n.node.addr, n.err.Error())
	}
	return fmt.Sprintf("error in docker node %q running command %q: %s", n.node.addr, n.cmd, n.err.Error())
}

func (n DockerNodeError) BaseError() error {
	return n.err
}

func wrapError(n node, err error) error {
	if err != nil {
		return DockerNodeError{node: n, err: err}
	}
	return nil
}

func wrapErrorWithCmd(n node, err error, cmd string) error {
	if err != nil {
		return DockerNodeError{node: n, err: err, cmd: cmd}
	}
	return nil
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
			_, err = c.Register(n.Address, n.Metadata)
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
func (c *Cluster) Register(address string, metadata map[string]string) (Node, error) {
	if address == "" {
		return Node{}, errors.New("Invalid address")
	}
	node := Node{
		Address:  address,
		Metadata: metadata,
	}
	return node, c.storage().StoreNode(node)
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

func (c *Cluster) WaitAndRegister(address string, metadata map[string]string, timeout time.Duration) (Node, error) {
	client, err := c.getNodeByAddr(address)
	if err != nil {
		return Node{}, err
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
		return Node{}, errors.New("timed out waiting for node to be ready")
	}
	return c.Register(address, metadata)
}

func (c *Cluster) runPingForHost(addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	client, err := c.getNodeByAddr(addr)
	if err != nil {
		log.Errorf("[active-monitoring]: error creating client: %s", err.Error())
		return
	}
	client.HTTPClient = pingClient
	err = client.Ping()
	if err == nil {
		c.handleNodeSuccess(addr)
	} else {
		log.Errorf("[active-monitoring]: error in ping: %s", err.Error())
		c.handleNodeError(addr, err, true)
	}
}

func (c *Cluster) runActiveMonitoring(updateInterval time.Duration) {
	log.Debugf("[active-monitoring]: active monitoring enabled, pinging hosts every %d seconds", updateInterval/time.Second)
	for {
		var nodes []Node
		var err error
		nodes, err = c.UnfilteredNodes()
		if err != nil {
			log.Errorf("[active-monitoring]: error in UnfilteredNodes: %s", err.Error())
		}
		wg := sync.WaitGroup{}
		for _, node := range nodes {
			wg.Add(1)
			go c.runPingForHost(node.Address, &wg)
		}
		wg.Wait()
		select {
		case <-c.monitoringDone:
			return
		case <-time.After(updateInterval):
		}
	}
}

func (c *Cluster) lockWithTimeout(addr string, isFailure bool) (func(), error) {
	lockTimeout := 3 * time.Minute
	locked, err := c.storage().LockNodeForHealing(addr, isFailure, lockTimeout)
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, errHealerInProgress
	}
	doneKeepAlive := make(chan bool)
	go func() {
		for {
			select {
			case <-doneKeepAlive:
				return
			case <-time.After(30 * time.Second):
			}
			c.storage().ExtendNodeLock(addr, lockTimeout)
		}
	}()
	return func() {
		doneKeepAlive <- true
		c.storage().UnlockNode(addr)
	}, nil
}

func (c *Cluster) handleNodeError(addr string, lastErr error, incrementFailures bool) error {
	unlock, err := c.lockWithTimeout(addr, true)
	if err != nil {
		return err
	}
	go func() {
		defer unlock()
		node, err := c.storage().RetrieveNode(addr)
		if err != nil {
			return
		}
		node.updateError(lastErr, incrementFailures)
		duration := c.healer.HandleError(&node)
		if duration > 0 {
			node.updateDisabled(time.Now().Add(duration))
		}
		c.storage().UpdateNode(node)
	}()
	return nil
}

func (c *Cluster) handleNodeSuccess(addr string) error {
	unlock, err := c.lockWithTimeout(addr, false)
	if err != nil {
		return err
	}
	defer unlock()
	node, err := c.storage().RetrieveNode(addr)
	if err != nil {
		return err
	}
	node.updateSuccess()
	return c.storage().UpdateNode(node)
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
		client, err := c.getNodeByAddr(addr)
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
				errChan <- wrapError(n, err)
			}
		}(client)
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

func clientWithTimeout(dialTimeout time.Duration, fullTimeout time.Duration) *http.Client {
	transport := http.Transport{
		Dial: (&net.Dialer{
			Timeout:   dialTimeout,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: dialTimeout,
	}
	return &http.Client{
		Transport: &transport,
		Timeout:   fullTimeout,
	}
}

func (c *Cluster) StopDryMode() {
	if c.dryServer != nil {
		c.dryServer.Stop()
	}
}

func (c *Cluster) DryMode() error {
	var err error
	c.dryServer, err = dtesting.NewServer("127.0.0.1:0", nil, nil)
	if err != nil {
		return err
	}
	oldStor := c.stor
	c.stor = &MapStorage{}
	nodes, err := oldStor.RetrieveNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		err = c.storage().StoreNode(node)
		if err != nil {
			return err
		}
	}
	images, err := oldStor.RetrieveImages()
	if err != nil {
		return err
	}
	for _, img := range images {
		for _, historyEntry := range img.History {
			if historyEntry.ImageId != img.LastId && historyEntry.Node != img.LastNode {
				err = c.PullImage(docker.PullImageOptions{
					Repository: img.Repository,
				}, docker.AuthConfiguration{}, historyEntry.Node)
				if err != nil {
					return err
				}
			}
		}
		err = c.PullImage(docker.PullImageOptions{
			Repository: img.Repository,
		}, docker.AuthConfiguration{}, img.LastNode)
		if err != nil {
			return err
		}
	}
	containers, err := oldStor.RetrieveContainers()
	if err != nil {
		return err
	}
	for _, container := range containers {
		err = c.storage().StoreContainer(container.Id, container.Host)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) getNodeByAddr(address string) (node, error) {
	if c.dryServer != nil {
		address = c.dryServer.URL()
	}
	var n node
	client, err := docker.NewClient(address)
	if err != nil {
		return n, err
	}
	client.HTTPClient = timeout10Client
	return node{addr: address, Client: client}, nil
}
