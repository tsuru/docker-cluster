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
// It returns the container, or an error, in case of failures.
func (c *Cluster) CreateContainer(config *docker.Config) (string, *docker.Container, error) {
	id, container, err := c.scheduler.Schedule(config)
	if err != nil {
		return id, container, err
	}
	if storage := c.storage(); storage != nil {
		storage.StoreContainer(container.ID, id)
	}
	return id, container, err
}

// InspectContainer returns information about a container by its ID, getting
// the information from the right node.
func (c *Cluster) InspectContainer(id string) (*docker.Container, error) {
	if node, err := c.getNodeForContainer(id); err == nil {
		return node.InspectContainer(id)
	} else if err != errStorageDisabled {
		return nil, err
	}
	container, err := c.runOnNodes(func(n node) (interface{}, error) {
		return n.InspectContainer(id)
	}, &dcli.NoSuchContainer{ID: id})
	if err != nil {
		return nil, err
	}
	return container.(*docker.Container), err
}

// KillContainer kills a container, returning an error in case of failure.
func (c *Cluster) KillContainer(id string) error {
	if node, err := c.getNodeForContainer(id); err == nil {
		return node.KillContainer(id)
	} else if err != errStorageDisabled {
		return err
	}
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.KillContainer(id)
	}, &dcli.NoSuchContainer{ID: id})
	return err
}

// ListContainers returns a slice of all containers in the cluster matching the
// given criteria.
func (c *Cluster) ListContainers(opts dcli.ListContainersOptions) ([]docker.APIContainers, error) {
	nodes, err := c.scheduler.Nodes()
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	result := make(chan []docker.APIContainers, len(nodes))
	errs := make(chan error, len(nodes))
	for _, n := range nodes {
		wg.Add(1)
		client, _ := dcli.NewClient(n.Address)
		go func(n node) {
			defer wg.Done()
			if containers, err := n.ListContainers(opts); err != nil {
				errs <- err
			} else {
				result <- containers
			}
		}(node{id: n.ID, Client: client})
	}
	wg.Wait()
	var group []docker.APIContainers
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

// RemoveContainer removes a container from the cluster.
func (c *Cluster) RemoveContainer(id string) error {
	err := c.removeFromStorage(id)
	if err != errStorageDisabled {
		return err
	}
	_, err = c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.RemoveContainer(id)
	}, &dcli.NoSuchContainer{ID: id})
	return err
}

func (c *Cluster) removeFromStorage(id string) error {
	if node, err := c.getNodeForContainer(id); err == nil {
		err = node.RemoveContainer(id)
		if err == nil {
			c.storage().RemoveContainer(id)
		}
		return err
	} else if err != errStorageDisabled {
		return err
	}
	return errStorageDisabled
}

func (c *Cluster) StartContainer(id string) error {
	if node, err := c.getNodeForContainer(id); err == nil {
		return node.StartContainer(id)
	} else if err != errStorageDisabled {
		return err
	}
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.StartContainer(id)
	}, &dcli.NoSuchContainer{ID: id})
	return err
}

// StopContainer stops a container, killing it after the given timeout, if it
// fails to stop nicely.
func (c *Cluster) StopContainer(id string, timeout uint) error {
	if node, err := c.getNodeForContainer(id); err == nil {
		return node.StopContainer(id, timeout)
	} else if err != errStorageDisabled {
		return err
	}
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.StopContainer(id, timeout)
	}, &dcli.NoSuchContainer{ID: id})
	return err
}

// RestartContainer restarts a container, killing it after the given timeout,
// if it fails to stop nicely.
func (c *Cluster) RestartContainer(id string, timeout uint) error {
	if node, err := c.getNodeForContainer(id); err == nil {
		return node.RestartContainer(id, timeout)
	} else if err != errStorageDisabled {
		return err
	}
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.RestartContainer(id, timeout)
	}, &dcli.NoSuchContainer{ID: id})
	return err
}

// WaitContainer blocks until the given container stops, returning the exit
// code of the container command.
func (c *Cluster) WaitContainer(id string) (int, error) {
	if node, err := c.getNodeForContainer(id); err == nil {
		return node.WaitContainer(id)
	} else if err != errStorageDisabled {
		return -1, err
	}
	exit, err := c.runOnNodes(func(n node) (interface{}, error) {
		return n.WaitContainer(id)
	}, &dcli.NoSuchContainer{ID: id})
	if err != nil {
		return -1, err
	}
	return exit.(int), nil
}

// AttachToContainer attaches to a container, using the given options.
func (c *Cluster) AttachToContainer(opts dcli.AttachToContainerOptions) error {
	if node, err := c.getNodeForContainer(opts.Container); err == nil {
		return node.AttachToContainer(opts)
	} else if err != errStorageDisabled {
		return err
	}
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.AttachToContainer(opts)
	}, &dcli.NoSuchContainer{ID: opts.Container})
	return err
}

// CommitContainer commits a container and returns the image id.
func (c *Cluster) CommitContainer(opts dcli.CommitContainerOptions) (*docker.Image, error) {
	if node, err := c.getNodeForContainer(opts.Container); err == nil {
		image, err := node.CommitContainer(opts)
		if err == nil {
			key := opts.Repository
			if key == "" {
				key = image.ID
			}
			c.storage().StoreImage(key, node.id)
		}
		return image, err
	} else if err != errStorageDisabled {
		return nil, err
	}
	image, err := c.runOnNodes(func(n node) (interface{}, error) {
		return n.CommitContainer(opts)
	}, &dcli.NoSuchContainer{ID: opts.Container})
	if err != nil {
		return nil, err
	}
	return image.(*docker.Image), nil
}

func (c *Cluster) getNodeForContainer(container string) (node, error) {
	return c.getNode(func(s Storage) (string, error) {
		return s.RetrieveContainer(container)
	})
}
