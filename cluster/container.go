// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/dotcloud/docker"
)

func (c *Cluster) CreateContainer(config *docker.Config) (*docker.Container, error) {
	return c.next().CreateContainer(config)
}
