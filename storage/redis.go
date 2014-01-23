// Copyright 2014 docker-cluster authors. All rights reserved.
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

var (
	ErrNoSuchNode      = errors.New("No such container")
	ErrNoSuchContainer = errors.New("No such container")
	ErrNoSuchImage     = errors.New("No such image")
)

type redisStorage struct {
	pool   *redis.Pool
	prefix string
}

func (s *redisStorage) key(value string) string {
	if s.prefix == "" {
		return value
	}
	return s.prefix + ":" + value
}

func (s *redisStorage) StoreContainer(container, host string) error {
	conn := s.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", s.key(container), host)
	return err
}

func (s *redisStorage) RetrieveContainer(container string) (string, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("GET", s.key(container))
	if err != nil {
		return "", err
	}
	if result == nil {
		return "", ErrNoSuchContainer
	}
	return string(result.([]byte)), nil
}

func (s *redisStorage) RemoveContainer(container string) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("DEL", s.key(container))
	if err != nil {
		return err
	}
	if result.(int64) < 1 {
		return ErrNoSuchContainer
	}
	return nil
}

func (s *redisStorage) StoreImage(image, host string) error {
	conn := s.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", s.key("image:"+image), host)
	return err
}

func (s *redisStorage) RetrieveImage(id string) (string, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("GET", s.key("image:"+id))
	if err != nil {
		return "", err
	}
	if result == nil {
		return "", ErrNoSuchImage
	}
	return string(result.([]byte)), nil
}

func (s *redisStorage) RemoveImage(id string) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("DEL", s.key("image:"+id))
	if err != nil {
		return err
	}
	if result.(int64) < 1 {
		return ErrNoSuchImage
	}
	return nil
}

func (s *redisStorage) StoreNode(id, address string) error {
	conn := s.pool.Get()
	defer conn.Close()
	_, err := conn.Do("LPUSH", s.key("nodes"), id)
	if err != nil {
		return err
	}
	_, err = conn.Do("SET", s.key("node:"+id), address)
	return err

}

func (s *redisStorage) RetrieveNode(id string) (string, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("GET", s.key("node:"+id))
	if err != nil {
		return "", err
	}
	if result == "" {
		return "", ErrNoSuchNode
	}
	return string(result.([]byte)), nil
}

func (s *redisStorage) RetrieveNodes() ([]cluster.Node, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("LRANGE", s.key("nodes"), 0, -1)
	if err != nil {
		return nil, err
	}
	items := result.([]interface{})
	nodes := make([]cluster.Node, len(items))
	for i, v := range items {
		id := string(v.([]byte))
		addr, _ := s.RetrieveNode(id)
		nodes[i] = cluster.Node{ID: id, Address: addr}
	}
	return nodes, nil
}

func (s *redisStorage) RemoveNode(id string) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("LREM", s.key("nodes"), id)
	result, err = conn.Do("DEL", s.key("node:"+id))
	if err != nil {
		return err
	}
	if result.(int64) < 1 {
		return ErrNoSuchNode
	}
	return nil
}

// Redis returns a storage instance that uses Redis to store nodes and
// containers relation.
//
// The addres must be in the format <host>:<port>. For servers that require
// authentication, use AuthenticatedRedis.
func Redis(addr, prefix string) cluster.Storage {
	return rStorage(addr, "", prefix)
}

// AuthenticatedRedis works like Redis, but supports password authentication.
func AuthenticatedRedis(addr, password, prefix string) cluster.Storage {
	return rStorage(addr, password, prefix)
}

func rStorage(addr, password, prefix string) cluster.Storage {
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
	}, 5)
	pool.IdleTimeout = 180e9
	return &redisStorage{pool: pool, prefix: prefix}
}
