// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/tsuru/docker-cluster/storage"
	"sync"
)

type MapStorage struct {
	cMap    map[string]string
	iMap    map[string]map[string]bool
	nodes   []Node
	nodeMap map[string]*Node
	cMut    sync.Mutex
	iMut    sync.Mutex
	nMut    sync.Mutex
}

func (s *MapStorage) StoreContainer(containerID, hostID string) error {
	s.cMut.Lock()
	defer s.cMut.Unlock()
	if s.cMap == nil {
		s.cMap = make(map[string]string)
	}
	s.cMap[containerID] = hostID
	return nil
}

func (s *MapStorage) RetrieveContainer(containerID string) (string, error) {
	s.cMut.Lock()
	defer s.cMut.Unlock()
	host, ok := s.cMap[containerID]
	if !ok {
		return "", storage.ErrNoSuchContainer
	}
	return host, nil
}

func (s *MapStorage) RemoveContainer(containerID string) error {
	s.cMut.Lock()
	defer s.cMut.Unlock()
	delete(s.cMap, containerID)
	return nil
}

func (s *MapStorage) StoreImage(imageID, hostID string) error {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	if s.iMap == nil {
		s.iMap = make(map[string]map[string]bool)
	}
	set, _ := s.iMap[imageID]
	if set == nil {
		set = make(map[string]bool)
		s.iMap[imageID] = set
	}
	set[hostID] = true
	return nil
}

func (s *MapStorage) RetrieveImage(imageID string) ([]string, error) {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	hostsSet, ok := s.iMap[imageID]
	if !ok {
		return nil, storage.ErrNoSuchImage
	}
	hosts := []string{}
	for host, _ := range hostsSet {
		hosts = append(hosts, host)
	}
	return hosts, nil
}

func (s *MapStorage) RemoveImage(imageID string) error {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	delete(s.iMap, imageID)
	return nil
}

func (s *MapStorage) updateNodeMap() {
	s.nodeMap = make(map[string]*Node)
	for i := range s.nodes {
		s.nodeMap[s.nodes[i].Address] = &s.nodes[i]
	}
}

func (s *MapStorage) StoreNode(node Node) error {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	for _, n := range s.nodes {
		if n.Address == node.Address {
			return storage.ErrDuplicatedNodeAddress
		}
	}
	if node.Metadata == nil {
		node.Metadata = make(map[string]string)
	}
	s.nodes = append(s.nodes, node)
	s.updateNodeMap()
	return nil
}

func (s *MapStorage) RetrieveNodes() ([]Node, error) {
	return s.nodes, nil
}

func (s *MapStorage) RetrieveNode(address string) (Node, error) {
	if s.nodeMap == nil {
		s.nodeMap = make(map[string]*Node)
	}
	node, ok := s.nodeMap[address]
	if !ok {
		return Node{}, storage.ErrNoSuchNode
	}
	return *node, nil
}

func (s *MapStorage) UpdateNode(node Node) error {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	if s.nodeMap == nil {
		s.nodeMap = make(map[string]*Node)
	}
	_, ok := s.nodeMap[node.Address]
	if !ok {
		return storage.ErrNoSuchNode
	}
	*s.nodeMap[node.Address] = node
	return nil
}

func (s *MapStorage) RetrieveNodesByMetadata(metadata map[string]string) ([]Node, error) {
	filteredNodes := []Node{}
	for _, node := range s.nodes {
		for key, value := range metadata {
			nodeVal, ok := node.Metadata[key]
			if ok && nodeVal == value {
				filteredNodes = append(filteredNodes, node)
			}
		}
	}
	return filteredNodes, nil
}

func (s *MapStorage) RemoveNode(addr string) error {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	index := -1
	for i, node := range s.nodes {
		if node.Address == addr {
			index = i
		}
	}
	if index < 0 {
		return storage.ErrNoSuchNode
	}
	copy(s.nodes[index:], s.nodes[index+1:])
	s.nodes = s.nodes[:len(s.nodes)-1]
	s.updateNodeMap()
	return nil
}

func (s *MapStorage) LockNodeForHealing(address string) (bool, error) {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	n, present := s.nodeMap[address]
	if !present || n.Healing {
		return false, nil
	}
	s.nodeMap[address].Healing = true
	return true, nil
}
