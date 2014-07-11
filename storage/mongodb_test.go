// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package storage

import (
	"testing"
)

func TestMongodbStorage(t *testing.T) {
	mongo, err := Mongodb("mongodb://localhost", "test-docker-cluster")
	assertIsNil(err, t)
	stor := mongo.(*mongodbStorage)
	err = stor.session.DB("test-docker-cluster").DropDatabase()
	assertIsNil(err, t)
	runTestsForStorage(mongo, t)
}
