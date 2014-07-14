// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"fmt"
	"github.com/fsouza/go-dockerclient"
	"sync"
)

type containerList []docker.APIContainers

func (l containerList) Len() int {
	return len(l)
}

func (l containerList) Less(i, j int) bool {
	return l[i].ID < l[j].ID
}

func (l containerList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type mapStorage struct {
	cMap  map[string]string
	iMap  map[string][]string
	nodes []Node
	cMut  sync.Mutex
	iMut  sync.Mutex
	nMut  sync.Mutex
}

func (s *mapStorage) StoreContainer(containerID, hostID string) error {
	s.cMut.Lock()
	defer s.cMut.Unlock()
	if s.cMap == nil {
		s.cMap = make(map[string]string)
	}
	s.cMap[containerID] = hostID
	return nil
}

func (s *mapStorage) RetrieveContainer(containerID string) (string, error) {
	s.cMut.Lock()
	defer s.cMut.Unlock()
	host, ok := s.cMap[containerID]
	if !ok {
		return "", &docker.NoSuchContainer{ID: containerID}
	}
	return host, nil
}

func (s *mapStorage) RemoveContainer(containerID string) error {
	s.cMut.Lock()
	defer s.cMut.Unlock()
	delete(s.cMap, containerID)
	return nil
}

func (s *mapStorage) StoreImage(imageID, hostID string) error {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	if s.iMap == nil {
		s.iMap = make(map[string][]string)
	}
	s.iMap[imageID] = append(s.iMap[imageID], hostID)
	return nil
}

func (s *mapStorage) RetrieveImage(imageID string) ([]string, error) {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	hosts, ok := s.iMap[imageID]
	if !ok {
		return nil, docker.ErrNoSuchImage
	}
	return hosts, nil
}

func (s *mapStorage) RemoveImage(imageID string) error {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	delete(s.iMap, imageID)
	return nil
}

func (s *mapStorage) StoreNode(node Node) error {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	for _, n := range s.nodes {
		if n.Address == node.Address {
			return ErrDuplicatedNodeAddress
		}
	}
	s.nodes = append(s.nodes, node)
	return nil
}

func (s *mapStorage) RetrieveNodes() ([]Node, error) {
	return s.nodes, nil
}

func (s *mapStorage) RetrieveNodesByMetadata(metadata map[string]string) ([]Node, error) {
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

func (s *mapStorage) RemoveNode(addr string) error {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	index := -1
	for i, node := range s.nodes {
		if node.Address == addr {
			index = i
		}
	}
	if index < 0 {
		return errors.New("no such node")
	}
	copy(s.nodes[index:], s.nodes[index+1:])
	s.nodes = s.nodes[:len(s.nodes)-1]
	return nil
}

type failingStorage struct{}

func (failingStorage) StoreContainer(container, host string) error {
	return errors.New("storage error")
}
func (failingStorage) RetrieveContainer(container string) (string, error) {
	return "", errors.New("storage error")
}
func (failingStorage) RemoveContainer(container string) error {
	return errors.New("storage error")
}
func (failingStorage) StoreImage(image, host string) error {
	return errors.New("storage error")
}
func (failingStorage) RetrieveImage(image string) ([]string, error) {
	return nil, errors.New("storage error")
}
func (failingStorage) RemoveImage(image string) error {
	return errors.New("storage error")
}
func (failingStorage) StoreNode(node Node) error {
	return errors.New("storage error")
}
func (failingStorage) RetrieveNodesByMetadata(metadata map[string]string) ([]Node, error) {
	return nil, errors.New("storage error")
}
func (failingStorage) RetrieveNodes() ([]Node, error) {
	return nil, errors.New("storage error")
}
func (failingStorage) RemoveNode(address string) error {
	return errors.New("storage error")
}

type fakeScheduler struct{}

func (fakeScheduler) Schedule(c *Cluster, opts docker.CreateContainerOptions, schedulerOpts SchedulerOptions) (Node, error) {
	return Node{}, nil
}

type failingScheduler struct{}

func (failingScheduler) Schedule(c *Cluster, opts docker.CreateContainerOptions, schedulerOpts SchedulerOptions) (Node, error) {
	return Node{}, errors.New("Cannot schedule")
}

type optsScheduler struct {
	roundRobin
}

func (s optsScheduler) Schedule(c *Cluster, opts docker.CreateContainerOptions, schedulerOpts SchedulerOptions) (Node, error) {
	optStr := schedulerOpts.(string)
	if optStr != "myOpt" {
		return Node{}, fmt.Errorf("Invalid option %s", optStr)
	}
	return s.roundRobin.Schedule(c, opts, schedulerOpts)
}
