// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redis

import (
	"github.com/garyburd/redigo/redis"
	"github.com/tsuru/docker-cluster/cluster"
	"github.com/tsuru/docker-cluster/storage"
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
		return "", storage.ErrNoSuchContainer
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
		return storage.ErrNoSuchContainer
	}
	return nil
}

func (s *redisStorage) StoreImage(image, host string) error {
	conn := s.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SADD", s.key("image:"+image), host)
	return err
}

func (s *redisStorage) RetrieveImage(id string) ([]string, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SMEMBERS", s.key("image:"+id))
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, storage.ErrNoSuchImage
	}
	items := result.([]interface{})
	if len(items) == 0 {
		return nil, storage.ErrNoSuchImage
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
		return storage.ErrNoSuchImage
	}
	return nil
}

func (s *redisStorage) saveNode(node cluster.Node) error {
	conn := s.pool.Get()
	defer conn.Close()
	_, err := conn.Do("SADD", s.key("nodes"), node.Address)
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
	_, err = conn.Do("DEL", args[0])
	if err != nil {
		return err
	}
	_, err = conn.Do("HMSET", args...)
	if err != nil {
		return err
	}
	if !node.Healing {
		_, err = conn.Do("DEL", s.key("node:healing:"+node.Address))
	}
	return err
}

func (s *redisStorage) StoreNode(node cluster.Node) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SISMEMBER", s.key("nodes"), node.Address)
	if err != nil {
		return err
	}
	if result.(int64) != 0 {
		return storage.ErrDuplicatedNodeAddress
	}
	return s.saveNode(node)
}

func (s *redisStorage) retrieveNode(conn redis.Conn, address string) (cluster.Node, error) {
	result, err := conn.Do("HGETALL", s.key("node:metadata:"+address))
	if err != nil {
		return cluster.Node{}, err
	}
	metadata := make(map[string]string)
	if result != nil {
		metaItems := result.([]interface{})
		for i := 0; i < len(metaItems); i += 2 {
			key, value := string(metaItems[i].([]byte)), string(metaItems[i+1].([]byte))
			metadata[key] = value
		}
	}
	node := cluster.Node{Address: address, Metadata: metadata}
	result, err = conn.Do("GET", s.key("node:healing:"+address))
	if err != nil {
		return cluster.Node{}, err
	}
	if result != nil && string(result.([]byte)) == "1" {
		node.Healing = true
	}
	return node, nil
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
		nodes[i], err = s.retrieveNode(conn, address)
		if err != nil {
			return nil, err
		}
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
		return storage.ErrNoSuchNode
	}
	_, err = conn.Do("DEL", s.key("node:metadata:"+address))
	return err
}

func (s *redisStorage) RetrieveNode(address string) (cluster.Node, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SISMEMBER", s.key("nodes"), address)
	if err != nil {
		return cluster.Node{}, err
	}
	if result.(int64) == 0 {
		return cluster.Node{}, storage.ErrNoSuchNode
	}
	return s.retrieveNode(conn, address)
}

func (s *redisStorage) UpdateNode(node cluster.Node) error {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SISMEMBER", s.key("nodes"), node.Address)
	if err != nil {
		return err
	}
	if result.(int64) == 0 {
		return storage.ErrNoSuchNode
	}
	return s.saveNode(node)
}

func (s *redisStorage) LockNodeForHealing(address string) (bool, error) {
	conn := s.pool.Get()
	defer conn.Close()
	result, err := conn.Do("SETNX", s.key("node:healing:"+address), "1")
	if err != nil {
		return false, err
	}
	if result.(int64) == 0 {
		return false, nil
	}
	return true, nil
}

// Redis returns a cluster.storage instance that uses Redis to store nodes and
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
