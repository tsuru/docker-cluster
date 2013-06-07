// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/fsouza/go-dockerclient"
	"sync/atomic"
)

type Node struct {
	Id      string
	Address string
}

type node struct {
	*docker.Client
	id   string
	load int64
}

type Cluster struct {
	nodes    []node
	lastUsed int64
}

func New(nodes ...Node) (*Cluster, error) {
	c := Cluster{
		nodes:    make([]node, len(nodes)),
		lastUsed: -1,
	}
	for i, n := range nodes {
		client, err := docker.NewClient(n.Address)
		if err != nil {
			return nil, err
		}
		c.nodes[i] = node{
			id:     n.Id,
			Client: client,
		}
	}
	return &c, nil
}

func (c *Cluster) next() node {
	index := atomic.AddInt64(&c.lastUsed, 1) % int64(len(c.nodes))
	return c.nodes[index]
}
