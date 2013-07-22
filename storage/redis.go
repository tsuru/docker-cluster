// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package storage provides some implementations of the Storage interface,
// defined in the cluster package.
package storage

import (
	"github.com/garyburd/redigo/redis"
	"github.com/globocom/docker-cluster/cluster"
)

type redisStorage struct {
	pool *redis.Pool
}

func (s *redisStorage) Store(container, host string) error {
	conn := s.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", container, host)
	return err
}

func (s *redisStorage) Retrieve(container string) (string, error) {
	return "", nil
}

// Redis returns a storage instance that uses Redis to store nodes and
// containers relation.
//
// The addres must be in the format <host>:<port>. For servers that require
// authentication, use AuthenticatedRedis.
func Redis(addr string) (cluster.Storage, error) {
	return nil, nil
}

// AuthenticatedRedis works like Redis, but supports password authentication.
func AuthenticatedRedis(addr, password string) (cluster.Storage, error) {
	return nil, nil
}
