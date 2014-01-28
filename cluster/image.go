// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/fsouza/go-dockerclient"
)

// RemoveImage removes an image from all nodes in the cluster, returning an
// error in case of failure.
func (c *Cluster) RemoveImage(name string) error {
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.RemoveImage(name)
	}, docker.ErrNoSuchImage, false)
	return err
}

// PullImage pulls an image from a remote registry server, returning an error
// in case of failure.
//
// It will pull all images in parallel, so users need to make sure that the
// given buffer is safe.
func (c *Cluster) PullImage(opts docker.PullImageOptions) error {
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.PullImage(opts)
	}, docker.ErrNoSuchImage, true)
	return err
}

// PushImage pushes an image to a remote registry server, returning an error in
// case of failure.
func (c *Cluster) PushImage(opts docker.PushImageOptions, auth docker.AuthConfiguration) error {
	if node, err := c.getNodeForImage(opts.Name); err == nil {
		return node.PushImage(opts, auth)
	} else if err != errStorageDisabled {
		return err
	}
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.PushImage(opts, auth)
	}, docker.ErrNoSuchImage, false)
	return err
}

func (c *Cluster) getNodeForImage(image string) (node, error) {
	return c.getNode(func(s Storage) (string, error) {
		return s.RetrieveImage(image)
	})
}

// ImportImage imports an image from a url or stdin
func (c *Cluster) ImportImage(opts docker.ImportImageOptions) error {
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.ImportImage(opts)
	}, docker.ErrNoSuchImage, false)
	return err
}
