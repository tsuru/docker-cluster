// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package storage provides some implementations of the Storage interface,
// defined in the cluster package.
package storage

import (
	"errors"
	"github.com/garyburd/redigo/redis"
	"github.com/globocom/docker-cluster/cluster"
)

var ErrNoSuchContainer = errors.New("No such container")

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
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("GET", container)
	if err != nil {
		return "", err
	}
	return string(result.([]byte)), nil
}

func (s *redisStorage) Remove(container string) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("DEL", container)
	if err != nil {
		return err
	}
	if result.(int64) < 1 {
		return ErrNoSuchContainer
	}
	return nil
}

// Redis returns a storage instance that uses Redis to store nodes and
// containers relation.
//
// The addres must be in the format <host>:<port>. For servers that require
// authentication, use AuthenticatedRedis.
func Redis(addr string) cluster.Storage {
	return rStorage(addr, "")
}

// AuthenticatedRedis works like Redis, but supports password authentication.
func AuthenticatedRedis(addr, password string) cluster.Storage {
	return rStorage(addr, password)
}

func rStorage(addr, password string) cluster.Storage {
	pool := redis.NewPool(func() (redis.Conn, error) {
		conn, err := redis.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		if password != "" {
			_, err = conn.Do("AUTH", password)
			if err != nil {
				return nil, err
			}
		}
		return conn, nil
	}, 10)
	return &redisStorage{pool: pool}
}
