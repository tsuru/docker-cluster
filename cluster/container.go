// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
	dcli "github.com/fsouza/go-dockerclient"
	"sync"
)

// CreateContainer creates a container in one of the nodes.
//
// It returns the ID of the node where the container is running, and the
// container, or an error, in case of failures. It will always return the ID of
// the node, even in case of failures.
func (c *Cluster) CreateContainer(config *docker.Config) (string, *docker.Container, error) {
	node := c.next()
	container, err := node.CreateContainer(config)
	return node.id, container, err
}

// InspectContainer returns information about a container by its ID, getting
// the information from the right node.
func (c *Cluster) InspectContainer(id string) (*docker.Container, error) {
	return c.runOnNodes(func(n node) (*docker.Container, error) {
		return n.InspectContainer(id)
	})
}

// KillContainer kills a container, returning an error in case of failure.
func (c *Cluster) KillContainer(id string) error {
	_, err := c.runOnNodes(func(n node) (*docker.Container, error) {
		return nil, n.KillContainer(id)
	})
	return err
}

// ListContainers returns a slice of all containers in the cluster matching the
// given criteria.
func (c *Cluster) ListContainers(opts *dcli.ListContainersOptions) ([]docker.APIContainers, error) {
	c.mut.RLock()
	defer c.mut.RUnlock()
	var wg sync.WaitGroup
	result := make(chan []docker.APIContainers, len(c.nodes))
	errs := make(chan error, len(c.nodes))
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n node) {
			defer wg.Done()
			if containers, err := n.ListContainers(opts); err != nil {
				errs <- err
			} else {
				result <- containers
			}
		}(n)
	}
	wg.Wait()
	var group []docker.APIContainers
	var err error
	for {
		select {
		case containers := <-result:
			group = append(group, containers...)
		case err = <-errs:
		default:
			return group, err
		}
	}
}

type containerFunc func(node) (*docker.Container, error)

func (c *Cluster) runOnNodes(fn containerFunc) (*docker.Container, error) {
	c.mut.RLock()
	defer c.mut.RUnlock()
	var wg sync.WaitGroup
	finish := make(chan int8, 1)
	errChan := make(chan error, len(c.nodes))
	result := make(chan *docker.Container, 1)
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n node) {
			defer wg.Done()
			container, err := fn(n)
			if err == nil {
				result <- container
			} else if err != dcli.ErrNoSuchContainer {
				errChan <- err
			}
		}(n)
	}
	go func() {
		wg.Wait()
		close(finish)
	}()
	select {
	case container := <-result:
		return container, nil
	case err := <-errChan:
		return nil, err
	case <-finish:
		return nil, dcli.ErrNoSuchContainer
	}
}
