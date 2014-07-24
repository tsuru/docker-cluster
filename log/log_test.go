// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package log

import (
	"bytes"
	"strings"
	"testing"
)

func TestDefaultsToStdErr(t *testing.T) {
	defer func() {
		if val := recover(); val != nil {
			t.Fatalf("Expected not to panic, got: %#v", val)
		}
	}()
	Debugf("%s - %s - %d", "foo", "bar", 1)
}

func TestDebugf(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetDebug(true)
	Debugf("%s - %s - %d", "foo", "bar", 1)
	expected := "[docker-cluster][debug] foo - bar - 1\n"
	if !strings.Contains(buf.String(), expected) {
		t.Fatalf("Expected log to be %q, got: %q", expected, buf.String())
	}
}

func TestDebugfWithoutDebug(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetDebug(false)
	Debugf("%s - %s - %d", "foo", "bar", 1)
	expected := ""
	if !strings.Contains(buf.String(), expected) {
		t.Fatalf("Expected log to be %q, got: %q", expected, buf.String())
	}
}

func TestErrorf(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetDebug(false)
	Errorf("%s - %s - %d", "foo", "bar", 1)
	expected := "[docker-cluster][error] foo - bar - 1\n"
	if !strings.Contains(buf.String(), expected) {
		t.Fatalf("Expected log to be %q, got: %q", expected, buf.String())
	}
}
