// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redis

import (
	"testing"

	storageTesting "github.com/tsuru/docker-cluster/storage/testing"
)

func TestRedisStorage(t *testing.T) {
	redis := Redis("localhost:6379", "test-docker-cluster")
	stor := redis.(*redisStorage)
	conn := stor.pool.Get()
	defer conn.Close()
	result, err := conn.Do("KEYS", "test-docker-cluster*")
	if err != nil {
		t.Fatal(err)
	}
	keys := result.([]interface{})
	for _, key := range keys {
		keyName := string(key.([]byte))
		conn.Do("DEL", keyName)
	}
	storageTesting.RunTestsForStorage(redis, t)
}
