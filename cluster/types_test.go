// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"github.com/dotcloud/docker"
	dcli "github.com/fsouza/go-dockerclient"
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
	cMap map[string]string
	iMap map[string]string
	nMap map[string]string
	cMut sync.Mutex
	iMut sync.Mutex
	nMut sync.Mutex
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
		return "", &dcli.NoSuchContainer{ID: containerID}
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
		s.iMap = make(map[string]string)
	}
	s.iMap[imageID] = hostID
	return nil
}

func (s *mapStorage) RetrieveImage(imageID string) (string, error) {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	host, ok := s.iMap[imageID]
	if !ok {
		return "", dcli.ErrNoSuchImage
	}
	return host, nil
}

func (s *mapStorage) RemoveImage(imageID string) error {
	s.iMut.Lock()
	defer s.iMut.Unlock()
	delete(s.iMap, imageID)
	return nil
}

func (s *mapStorage) StoreNode(id, address string) error {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	if s.nMap == nil {
		s.nMap = make(map[string]string)
	}
	s.nMap[id] = address
	return nil
}

func (s *mapStorage) RemoveNode(id string) error {
	s.nMut.Lock()
	defer s.nMut.Unlock()
	delete(s.nMap, id)
	return nil
}

type fakeScheduler struct{}

func (fakeScheduler) Schedule(opts dcli.CreateContainerOptions, config *docker.Config) (string, *docker.Container, error) {
	return "", nil, nil
}

func (fakeScheduler) Nodes() ([]Node, error) {
	return nil, nil
}

type failingScheduler struct{}

func (failingScheduler) Schedule(opts dcli.CreateContainerOptions, config *docker.Config) (string, *docker.Container, error) {
	return "", nil, errors.New("Cannot schedule")
}

func (failingScheduler) Nodes() ([]Node, error) {
	return nil, errors.New("Cannot retrieve list of nodes")
}
