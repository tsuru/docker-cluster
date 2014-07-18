// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cluster provides types and functions for management of Docker
// clusters, scheduling container operations among hosts running Docker
// (nodes).
package cluster

import (
	"encoding/json"
	"strconv"
	"time"
)

// Node represents a host running Docker. Each node has an Address
// (in the form <scheme>://<host>:<port>/) and map with arbritary
// metadata.
type Node struct {
	Address  string `bson:"_id"`
	Metadata map[string]string
	Healing  bool
}

type NodeList []Node

const (
	NodeStatusWaiting  = "waiting"
	NodeStatusReady    = "ready"
	NodeStatusRetry    = "ready for retry"
	NodeStatusDisabled = "disabled"
	NodeStatusHealing  = "healing"
)

func (a NodeList) Len() int           { return len(a) }
func (a NodeList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a NodeList) Less(i, j int) bool { return a[i].Address < a[j].Address }

func (n Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"Address":  n.Address,
		"Metadata": n.Metadata,
		"Status":   n.Status(),
	})
}

func (n *Node) Status() string {
	if n.Healing {
		return NodeStatusHealing
	}
	if n.Metadata == nil {
		return NodeStatusWaiting
	}
	if n.isEnabled() {
		_, hasFailures := n.Metadata["Failures"]
		if hasFailures {
			return NodeStatusRetry
		}
		_, hasSuccess := n.Metadata["LastSuccess"]
		if !hasSuccess {
			return NodeStatusWaiting
		}
		return NodeStatusReady
	}
	return NodeStatusDisabled
}

func (n *Node) FailureCount() int {
	if n.Metadata == nil {
		return 0
	}
	metaFail, _ := n.Metadata["Failures"]
	failures, _ := strconv.Atoi(metaFail)
	return failures
}

func (n *Node) updateError(lastErr error) {
	if n.Metadata == nil {
		n.Metadata = make(map[string]string)
	}
	n.Metadata["Failures"] = strconv.Itoa(n.FailureCount() + 1)
	n.Metadata["LastError"] = lastErr.Error()
}

func (n *Node) updateDisabled(disabledUntil time.Time) {
	if n.Metadata == nil {
		n.Metadata = make(map[string]string)
	}
	n.Metadata["DisabledUntil"] = disabledUntil.Format(time.RFC3339)
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
	if n.Healing {
		return false
	}
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
