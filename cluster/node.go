// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cluster provides types and functions for management of Docker
// clusters, scheduling container operations among hosts running Docker
// (nodes).
package cluster

import (
	"strconv"
	"time"
)

// Node represents a host running Docker. Each node has an Address
// (in the form <scheme>://<host>:<port>/) and map with arbritary
// metadata.
type Node struct {
	Address  string
	Metadata map[string]string
}

type NodeList []Node

func (a NodeList) Len() int           { return len(a) }
func (a NodeList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a NodeList) Less(i, j int) bool { return a[i].Address < a[j].Address }

func (n *Node) updateError(lastErr error, disabledUntil time.Time) {
	if n.Metadata == nil {
		n.Metadata = make(map[string]string)
	}
	metaFail, _ := n.Metadata["Failures"]
	failures, _ := strconv.Atoi(metaFail)
	failures++
	n.Metadata["Failures"] = strconv.Itoa(failures)
	n.Metadata["DisabledUntil"] = disabledUntil.Format(time.RFC3339)
	n.Metadata["LastError"] = lastErr.Error()
}

func (n *Node) updateSuccess() {
	if n.Metadata == nil {
		n.Metadata = make(map[string]string)
	}
	delete(n.Metadata, "Failures")
	delete(n.Metadata, "DisabledUntil")
	delete(n.Metadata, "LastError")
	n.Metadata["LastSuccess"] = time.Now().Format(time.RFC3339)
}

func (n *Node) isEnabled() bool {
	if n.Metadata == nil {
		return true
	}
	disabledStr, _ := n.Metadata["DisabledUntil"]
	t, _ := time.Parse(time.RFC3339, disabledStr)
	return time.Now().After(t)
}

func (nodes NodeList) filterDisabled() NodeList {
	filtered := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		if node.isEnabled() {
			filtered = append(filtered, node)
		}
	}
	return filtered
}
