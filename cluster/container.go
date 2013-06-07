// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
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
