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
	}, docker.ErrNoSuchImage)
	return err
}
