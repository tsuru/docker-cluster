// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package storage provides some implementations of the Storage interface,
// defined in the cluster package.
package storage

import (
	"github.com/garyburd/redigo/redis"
	"github.com/tsuru/docker-cluster/cluster"
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
	_, err := conn.Do("LPUSH", s.key("image:"+image), host)
	return err
}

func (s *redisStorage) RetrieveImage(id string) ([]string, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("LRANGE", s.key("image:"+id), 0, -1)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrNoSuchImage
	}
	items := result.([]interface{})
	if len(items) == 0 {
		return nil, ErrNoSuchImage
	}
	images := make([]string, len(items))
	for i, v := range items {
		images[i] = string(v.([]byte))
	}
	return images, nil
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

func (s *redisStorage) StoreNode(node cluster.Node) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SISMEMBER", s.key("nodes"), node.Address)
	if err != nil {
		return err
	}
	if result.(int64) != 0 {
		return cluster.ErrDuplicatedNodeAddress
	}
	_, err = conn.Do("SADD", s.key("nodes"), node.Address)
	if err != nil {
		return err
	}
	if node.Metadata == nil {
		return nil
	}
	args := []interface{}{
		s.key("node:metadata:" + node.Address),
	}
	for key, value := range node.Metadata {
		args = append(args, key, value)
	}
	if len(args) == 1 {
		return nil
	}
	_, err = conn.Do("HMSET", args...)
	return err
}

func (s *redisStorage) RetrieveNodes() ([]cluster.Node, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SMEMBERS", s.key("nodes"))
	if err != nil {
		return nil, err
	}
	items := result.([]interface{})
	nodes := make([]cluster.Node, len(items))
	for i, v := range items {
		address := string(v.([]byte))
		result, err := conn.Do("HGETALL", s.key("node:metadata:"+address))
		if err != nil {
			return nil, err
		}
		var metadata map[string]string
		if result != nil {
			metadata = make(map[string]string)
			metaItems := result.([]interface{})
			for i := 0; i < len(metaItems); i += 2 {
				key, value := string(metaItems[i].([]byte)), string(metaItems[i+1].([]byte))
				metadata[key] = value
			}
		}
		nodes[i] = cluster.Node{Address: address, Metadata: metadata}
	}
	return nodes, nil
}

func (s *redisStorage) RetrieveNodesByMetadata(metadata map[string]string) ([]cluster.Node, error) {
	nodes, err := s.RetrieveNodes()
	if err != nil {
		return nil, err
	}
	filteredNodes := []cluster.Node{}
	for _, node := range nodes {
		for key, value := range metadata {
			nodeVal, ok := node.Metadata[key]
			if ok && nodeVal == value {
				filteredNodes = append(filteredNodes, node)
			}
		}
	}
	return filteredNodes, nil
}

func (s *redisStorage) RemoveNode(address string) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SREM", s.key("nodes"), address)
	if err != nil {
		return err
	}
	if result.(int64) < 1 {
		return ErrNoSuchNode
	}
	_, err = conn.Do("DEL", s.key("node:metadata:"+address))
	return err
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
