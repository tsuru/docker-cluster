// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"fmt"
	"github.com/fsouza/go-dockerclient"
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
func (failingStorage) RetrieveNode(addr string) (Node, error) {
	return Node{}, errors.New("storage error")
}
func (failingStorage) UpdateNode(node Node) error {
	return errors.New("storage error")
}
func (failingStorage) RemoveNode(address string) error {
	return errors.New("storage error")
}
func (failingStorage) LockNodeForHealing(address string) (bool, error) {
	return false, errors.New("storage error")
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
