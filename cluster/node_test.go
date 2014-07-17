// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"regexp"
	"testing"
	"time"
)

func TestNodeUpdateError(t *testing.T) {
	node := Node{}
	disabledUntil := time.Now().Add(5 * time.Minute)
	expectedErr := "some error"
	node.updateError(errors.New(expectedErr), disabledUntil)
	if node.Metadata["Failures"] != "1" {
		t.Fatalf("Expected failures counter 1, got: %s", node.Metadata["Failures"])
	}
	if node.Metadata["DisabledUntil"] != disabledUntil.Format(time.RFC3339) {
		t.Fatalf("Expected disabled until %q, got: %q",
			disabledUntil.Format(time.RFC3339), node.Metadata["DisabledUntil"])
	}
	if node.Metadata["LastError"] != expectedErr {
		t.Fatalf("Expected last error %q, got %q", expectedErr, node.Metadata["LastError"])
	}
	node.updateError(errors.New(expectedErr), disabledUntil)
	if node.Metadata["Failures"] != "2" {
		t.Fatalf("Expected failures counter 2, got: %s", node.Metadata["Failures"])
	}
}

func TestNodeUpdateSuccess(t *testing.T) {
	node := Node{Metadata: map[string]string{
		"Failures":      "9",
		"DisabledUntil": "something",
		"LastError":     "some error",
	}}
	now := time.Now().Format(time.RFC3339)
	node.updateSuccess()
	_, ok := node.Metadata["Failures"]
	if ok {
		t.Fatal("Node shouldn't have Failures")
	}
	_, ok = node.Metadata["DisabledUntil"]
	if ok {
		t.Fatal("Node shouldn't have DisabledUntil")
	}
	_, ok = node.Metadata["LastError"]
	if ok {
		t.Fatal("Node shouldn't have LastError")
	}
	re := regexp.MustCompile(`(.*T\d{2}:\d{2}).*`)
	lastSuccess := node.Metadata["LastSuccess"]
	lastSuccess = re.ReplaceAllString(lastSuccess, "$1")
	now = re.ReplaceAllString(now, "$1")
	if lastSuccess != now {
		t.Fatalf("Expected LastSuccess %q, got: %q", now, lastSuccess)
	}
}

func TestNodeIsEnabled(t *testing.T) {
	node := Node{}
	if !node.isEnabled() {
		t.Fatal("node should be enabled")
	}
	node = Node{Metadata: map[string]string{
		"DisabledUntil": time.Now().Add(-1 * time.Minute).Format(time.RFC3339),
	}}
	if !node.isEnabled() {
		t.Fatal("node should be enabled")
	}
	node = Node{Metadata: map[string]string{
		"DisabledUntil": time.Now().Add(1 * time.Minute).Format(time.RFC3339),
	}}
	if node.isEnabled() {
		t.Fatal("node should be disabled")
	}
}

func TestNodeListFilterDisabled(t *testing.T) {
	nodes := []Node{{Address: "a1"}, {Address: "a2"}, {Address: "a3"}}
	until := time.Now().Add(1 * time.Minute).Format(time.RFC3339)
	nodes[1].Metadata = map[string]string{"DisabledUntil": until}
	filtered := NodeList(nodes).filterDisabled()
	if len(filtered) != 2 {
		t.Fatalf("Expected filtered nodes len = 2, got %q", len(filtered))
	}
	if filtered[0].Address != "a1" || filtered[1].Address != "a3" {
		t.Fatalf("Expected filtered nodes to be %#v", filtered)
	}
}
