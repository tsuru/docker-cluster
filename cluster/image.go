// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/fsouza/go-dockerclient"
	"github.com/tsuru/docker-cluster/log"
)

// RemoveImage removes an image from the nodes where this images exists, returning an
// error in case of failure. Will wait for the image to be removed from all nodes.
func (c *Cluster) RemoveImageWait(name string) error {
	return c.removeImage(name, true)
}

// RemoveImage removes an image from the nodes where this images exists, returning an
// error in case of failure. Will wait for the image to be removed only from one node,
// removal from the other nodes will happen in background.
func (c *Cluster) RemoveImage(name string) error {
	return c.removeImage(name, false)
}

func (c *Cluster) removeImage(name string, waitForAll bool) error {
	hosts, err := c.storage().RetrieveImage(name)
	if err != nil {
		return err
	}
	_, err = c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.RemoveImage(name)
	}, docker.ErrNoSuchImage, waitForAll, hosts...)
	if err != nil && err != docker.ErrNoSuchImage {
		log.Debugf("Ignored error removing image from nodes: %s", err.Error())
	}
	return c.storage().RemoveImage(name)
}

func (c *Cluster) RemoveFromRegistry(imageId string) error {
	registryServer, imageTag := parseImageRegistry(imageId)
	if registryServer == "" {
		return nil
	}
	url := fmt.Sprintf("http://%s/v1/repositories/%s/tags", registryServer, imageTag)
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	_, err = http.DefaultClient.Do(request)
	return err
}

func parseImageRegistry(imageId string) (string, string) {
	parts := strings.SplitN(imageId, "/", 3)
	if len(parts) < 3 {
		return "", strings.Join(parts, "/")
	}
	return parts[0], strings.Join(parts[1:], "/")
}

// PullImage pulls an image from a remote registry server, returning an error
// in case of failure.
//
// It will pull all images in parallel, so users need to make sure that the
// given buffer is safe.
func (c *Cluster) PullImage(opts docker.PullImageOptions, auth docker.AuthConfiguration, nodes ...string) error {
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		key := opts.Repository
		c.storage().StoreImage(key, n.addr)
		return nil, n.PullImage(opts, auth)
	}, docker.ErrNoSuchImage, true, nodes...)
	return err
}

// PushImage pushes an image to a remote registry server, returning an error in
// case of failure.
func (c *Cluster) PushImage(opts docker.PushImageOptions, auth docker.AuthConfiguration) error {
	nodes, err := c.getNodesForImage(opts.Name)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		return node.PushImage(opts, auth)
	}
	return nil
}

func (c *Cluster) ListImages(all bool) ([]docker.APIImages, error) {
	nodes, err := c.Nodes()
	if err != nil {
		return nil, err
	}
	resultChan := make(chan []docker.APIImages, len(nodes))
	errChan := make(chan error, len(nodes))
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			client, err := c.getNodeByAddr(addr)
			if err != nil {
				errChan <- err
			}
			nodeImages, err := client.ListImages(all)
			if err != nil {
				errChan <- err
			}
			resultChan <- nodeImages
		}(node.Address)
	}
	wg.Wait()
	close(resultChan)
	select {
	case err := <-errChan:
		return nil, err
	default:
	}
	var allImages []docker.APIImages
	for images := range resultChan {
		allImages = append(allImages, images...)
	}
	return allImages, nil
}

func (c *Cluster) getNodesForImage(image string) ([]node, error) {
	var nodes []node
	hosts, err := c.storage().RetrieveImage(image)
	if err != nil {
		return nil, err
	}
	for _, host := range hosts {
		node, err := c.getNode(func(s Storage) (string, error) { return host, nil })
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, err
}

// ImportImage imports an image from a url or stdin
func (c *Cluster) ImportImage(opts docker.ImportImageOptions) error {
	_, err := c.runOnNodes(func(n node) (interface{}, error) {
		return nil, n.ImportImage(opts)
	}, docker.ErrNoSuchImage, false)
	return err
}

//BuildImage build an image and push it to register
func (c *Cluster) BuildImage(buildOptions docker.BuildImageOptions) error {
	nodes, err := c.Nodes()
	if err != nil {
		return err
	}
	if len(nodes) < 1 {
		return errors.New("There is no docker node. Please list one in tsuru.conf or add one with `tsuru docker-node-add`.")
	}
	nodeAddress := nodes[0].Address
	node, err := c.getNode(func(Storage) (string, error) {
		return nodeAddress, nil
	})
	if err != nil {
		return err
	}
	err = node.BuildImage(buildOptions)
	if err != nil {
		return err
	}
	return c.storage().StoreImage(buildOptions.Name, nodeAddress)
}
