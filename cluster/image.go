// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/fsouza/go-dockerclient"
	"io"
)

// RemoveImage removes an image from all nodes in the cluster, returning an
// error in case of failure.
func (c *Cluster) RemoveImage(name string) error {
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.RemoveImage(name)
	}, docker.ErrNoSuchImage)
	return err
}

// PullImage pulls an image from a remote registry server, returning an error
// in case of failure.
func (c *Cluster) PullImage(opts docker.PullImageOptions, w io.Writer) error {
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.PullImage(opts, w)
	}, docker.ErrNoSuchImage)
	return err
}

// PushImage pushes an image to a remote registry server, returning an error in
// case of failure.
func (c *Cluster) PushImage(opts docker.PushImageOptions, w io.Writer) error {
	if node, err := c.getNodeForImage(opts.Name); err == nil {
		return node.PushImage(opts, w)
	} else if err != errStorageDisabled {
		return err
	}
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.PushImage(opts, w)
	}, docker.ErrNoSuchImage)
	return err
}

func (c *Cluster) getNodeForImage(image string) (node, error) {
	return c.getNode(func(s Storage) (string, error) {
		return s.RetrieveImage(image)
	})
}
