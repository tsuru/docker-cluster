// Copyright 2013 Globo.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"testing"
)

func TestNewCluster(t *testing.T) {
	var tests = []struct {
		input []Node
		fail  bool
	}{
		{
			[]Node{{Id: "something", Address: "http://localhost:8083"}},
			false,
		},
		{
			[]Node{{Id: "something", Address: ""}, {Id: "otherthing", Address: "http://localhost:8083"}},
			true,
		},
	}
	for _, tt := range tests {
		_, err := New(tt.input...)
		if tt.fail && err == nil || !tt.fail && err != nil {
			t.Errorf("cluster.New(). Expect failure: %v. Got: %v.", tt.fail, err)
		}
	}
}
