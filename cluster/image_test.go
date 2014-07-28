// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"bytes"
	"github.com/fsouza/go-dockerclient"
	dtesting "github.com/fsouza/go-dockerclient/testing"
	"github.com/tsuru/docker-cluster/storage"
	"github.com/tsuru/tsuru/safe"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"testing"
)

func TestRemoveImage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server1.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	name := "tsuru/python"
	err = cluster.storage().StoreImage(name, server1.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RemoveImage(name)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("RemoveImage(%q): Did not call node HTTP server", name)
	}
	_, err = cluster.storage().RetrieveImage(name)
	if err != storage.ErrNoSuchImage {
		t.Errorf("RemoveImage(%q): wrong error. Want %#v. Got %#v.", name, storage.ErrNoSuchImage, err)
	}
}

func TestRemoveImageNotFoundInStorage(t *testing.T) {
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server1.Close()
	cluster, err := New(nil, &MapStorage{}, Node{Address: server1.URL})
	if err != nil {
		t.Fatal(err)
	}
	name := "tsuru/python"
	err = cluster.RemoveImage(name)
	if err != storage.ErrNoSuchImage {
		t.Errorf("RemoveImage(%q): wrong error. Want %#v. Got %#v.", name, storage.ErrNoSuchImage, err)
	}
	if called {
		t.Errorf("RemoveImage(%q): server should not be called.", name)
	}
}

func TestRemoveImageNotFoundInServer(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server1.Close()
	name := "tsuru/python"
	stor := &MapStorage{}
	err := stor.StoreImage(name, server1.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, stor,
		Node{Address: server1.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RemoveImage(name)
	if err == nil || err != docker.ErrNoSuchImage {
		t.Errorf("RemoveImage(%q): wrong error. Want %s. Got %#v.", name, docker.ErrNoSuchImage, err)
	}
	_, err = cluster.storage().RetrieveImage(name)
	if err != storage.ErrNoSuchImage {
		t.Errorf("RemoveImage(%q): wrong error. Want %#v. Got %#v.", name, storage.ErrNoSuchImage, err)
	}
}

func TestRemoveImageNodeNotInStorage(t *testing.T) {
	called := false
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server1.Close()
	name := "tsuru/python"
	stor := &MapStorage{}
	err := stor.StoreImage(name, server1.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, stor)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RemoveImage(name)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("RemoveImage(%q): Did not call node HTTP server", name)
	}
	_, err = cluster.storage().RetrieveImage(name)
	if err != storage.ErrNoSuchImage {
		t.Errorf("RemoveImage(%q): wrong error. Want %#v. Got %#v.", name, storage.ErrNoSuchImage, err)
	}
}

func TestRemoveImageRemoveFromRegistry(t *testing.T) {
	var repoRequest http.Request
	repoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		repoRequest = *r
	}))
	defer repoServer.Close()
	u, _ := url.Parse(repoServer.URL)
	imageRepo := u.Host + "/tsuru/python"
	var called bool
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server1.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.storage().StoreImage(imageRepo, server1.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.RemoveImage(imageRepo)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Errorf("RemoveImage(%q): Did not call node HTTP server", imageRepo)
	}
	_, err = cluster.storage().RetrieveImage(imageRepo)
	if err != storage.ErrNoSuchImage {
		t.Errorf("RemoveImage(%q): wrong error. Want %#v. Got %#v.", imageRepo, storage.ErrNoSuchImage, err)
	}
	if repoRequest.Method != "DELETE" {
		t.Fatalf("removeFromRegistry(%q): Expected method to be DELETE, got: %s", imageRepo, repoRequest.Method)
	}
	path := "/v1/repositories/tsuru/python/tags"
	if repoRequest.URL.Path != path {
		t.Fatalf("removeFromRegistry(%q): Expected path to be %q, got: %s", imageRepo, path, repoRequest.URL.Path)
	}
}

func TestRemoveFromRegistry(t *testing.T) {
	var request http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request = *r
	}))
	defer server.Close()
	u, _ := url.Parse(server.URL)
	imageRepo := u.Host + "/tsuru/python"
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.removeFromRegistry(imageRepo)
	if err != nil {
		t.Fatal(err)
	}
	if request.Method != "DELETE" {
		t.Fatalf("removeFromRegistry(%q): Expected method to be DELETE, got: %s", imageRepo, request.Method)
	}
	path := "/v1/repositories/tsuru/python/tags"
	if request.URL.Path != path {
		t.Fatalf("removeFromRegistry(%q): Expected path to be %q, got: %s", imageRepo, path, request.URL.Path)
	}
}

func TestRemoveFromRegistryIgnoresNoRepo(t *testing.T) {
	imageRepo := "tsuru/python"
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.removeFromRegistry(imageRepo)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemoveFromRegistryErrorWithInvalidServer(t *testing.T) {
	imageRepo := "xxx.xxx.xxxxxxx/tsuru/python"
	cluster, err := New(nil, &MapStorage{})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.removeFromRegistry(imageRepo)
	if err == nil {
		t.Fatal("Expected error to be not nil, got nil")
	}
}

func TestPullImage(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 1!"))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 2!"))
	}))
	defer server2.Close()
	var buf safe.Buffer
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.PullImageOptions{Repository: "tsuru/python", OutputStream: &buf}
	err = cluster.PullImage(opts, docker.AuthConfiguration{})
	if err != nil {
		t.Error(err)
	}
	alternatives := []string{
		"Pulling from 1!Pulling from 2!",
		"Pulling from 2!Pulling from 1!",
	}
	if r := buf.String(); r != alternatives[0] && r != alternatives[1] {
		t.Errorf("Wrong output: Want %q. Got %q.", "Pulling from 1!Pulling from 2!", buf.String())
	}
	nodes, err := cluster.storage().RetrieveImage("tsuru/python")
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{server1.URL, server2.URL}
	sort.Strings(nodes)
	sort.Strings(expected)
	if !reflect.DeepEqual(nodes, expected) {
		t.Errorf("Wrong output: Want %q. Got %q.", expected, nodes)
	}
}

func TestPullImageNotFound(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	opts := docker.PullImageOptions{Repository: "tsuru/python", OutputStream: &buf}
	err = cluster.PullImage(opts, docker.AuthConfiguration{})
	if err == nil {
		t.Error("PullImage: got unexpected <nil> error")
	}
}

func TestPullImageSpecifyNode(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 1!"))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 2!"))
	}))
	defer server2.Close()
	var buf safe.Buffer
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.PullImageOptions{Repository: "tsuru/python", OutputStream: &buf}
	err = cluster.PullImage(opts, docker.AuthConfiguration{}, server2.URL)
	if err != nil {
		t.Error(err)
	}
	expected := "Pulling from 2!"
	if r := buf.String(); r != expected {
		t.Errorf("Wrong output: Want %q. Got %q.", expected, r)
	}
}

func TestPullImageSpecifyMultipleNodes(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 1!"))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 2!"))
	}))
	defer server2.Close()
	server3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pulling from 3!"))
	}))
	defer server3.Close()
	var buf safe.Buffer
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
		Node{Address: server3.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.PullImageOptions{Repository: "tsuru/python", OutputStream: &buf}
	err = cluster.PullImage(opts, docker.AuthConfiguration{}, server2.URL, server3.URL)
	if err != nil {
		t.Error(err)
	}
	alternatives := []string{
		"Pulling from 2!Pulling from 3!",
		"Pulling from 3!Pulling from 2!",
	}
	if r := buf.String(); r != alternatives[0] && r != alternatives[1] {
		t.Errorf("Wrong output: Want %q. Got %q.", "Pulling from 2!Pulling from 3!", r)
	}
}

func TestPushImage(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pushing to server 1!"))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Pushing to server 2!"))
	}))
	defer server2.Close()
	var buf safe.Buffer
	stor := &MapStorage{}
	err := stor.StoreImage("tsuru/ruby", server1.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, stor,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var auth docker.AuthConfiguration
	err = cluster.PushImage(docker.PushImageOptions{Name: "tsuru/ruby", OutputStream: &buf}, auth)
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`^Pushing to server \d`)
	if !re.MatchString(buf.String()) {
		t.Errorf("Wrong output: Want %q. Got %q.", "Pushing to server [12]", buf.String())
	}
}

func TestPushImageNotFound(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "No such image", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	var auth docker.AuthConfiguration
	err = cluster.PushImage(docker.PushImageOptions{Name: "tsuru/python", OutputStream: &buf}, auth)
	if err == nil {
		t.Error("PushImage: got unexpected <nil> error")
	}
}

func TestPushImageWithStorage(t *testing.T) {
	var count int
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pushed"))
	}))
	defer server2.Close()
	stor := MapStorage{}
	err := stor.StoreImage("tsuru/python", server2.URL)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := New(nil, &stor,
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	var auth docker.AuthConfiguration
	err = cluster.PushImage(docker.PushImageOptions{Name: "tsuru/python", OutputStream: &buf}, auth)
	if err != nil {
		t.Error(err)
	}
	if count > 0 {
		t.Error("PushImage with storage: should not send request to all servers, but did.")
	}
}

func TestImportImage(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("importing from 1"))
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("importing from 2"))
	}))
	defer server2.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf safe.Buffer
	opts := docker.ImportImageOptions{
		Repository:   "tsuru/python",
		Source:       "http://url.to/tar",
		OutputStream: &buf,
	}
	err = cluster.ImportImage(opts)
	if err != nil {
		t.Error(err)
	}
	re := regexp.MustCompile(`^importing from \d`)
	if !re.MatchString(buf.String()) {
		t.Errorf("Wrong output: Want %q. Got %q.", "importing from [12]", buf.String())
	}
}

func TestImportImageWithAbsentFile(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "file not found", http.StatusNotFound)
	}))
	defer server1.Close()
	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "file not found", http.StatusNotFound)
	}))
	defer server2.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
		Node{Address: server2.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf safe.Buffer
	opts := docker.ImportImageOptions{
		Repository:   "tsuru/python",
		Source:       "/path/to/tar",
		OutputStream: &buf,
	}
	err = cluster.ImportImage(opts)
	if err == nil {
		t.Error("ImportImage: got unexpected <nil> error")
	}
}

func TestBuildImage(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	}))
	defer server1.Close()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL},
	)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	buildOptions := docker.BuildImageOptions{
		Name:         "tsuru/python",
		Remote:       "http://localhost/Dockerfile",
		InputStream:  nil,
		OutputStream: &buf,
	}
	err = cluster.BuildImage(buildOptions)
	if err != nil {
		t.Error(err)
	}
	_, err = cluster.storage().RetrieveImage("tsuru/python")
	if err != nil {
		t.Error(err)
	}
}

type APIImagesList []docker.APIImages

func (a APIImagesList) Len() int           { return len(a) }
func (a APIImagesList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a APIImagesList) Less(i, j int) bool { return a[i].RepoTags[0] < a[j].RepoTags[0] }

func TestListImages(t *testing.T) {
	server1, err := dtesting.NewServer("127.0.0.1:0", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server1.Stop()
	server2, err := dtesting.NewServer("127.0.0.1:0", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server2.Stop()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL()},
		Node{Address: server2.URL()},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.PullImageOptions{Repository: "tsuru/python1"}
	err = cluster.PullImage(opts, docker.AuthConfiguration{}, server1.URL())
	if err != nil {
		t.Error(err)
	}
	opts = docker.PullImageOptions{Repository: "tsuru/python2"}
	err = cluster.PullImage(opts, docker.AuthConfiguration{}, server2.URL())
	if err != nil {
		t.Error(err)
	}
	images, err := cluster.ListImages(true)
	if err != nil {
		t.Fatal(err)
	}
	if len(images) != 2 {
		t.Fatalf("Expected images count to be 2, got: %d", len(images))
	}
	sort.Sort(APIImagesList(images))
	if images[0].RepoTags[0] != "tsuru/python1" {
		t.Fatalf("Expected images tsuru/python1, got: %s", images[0].RepoTags[0])
	}
	if images[1].RepoTags[0] != "tsuru/python2" {
		t.Fatalf("Expected images tsuru/python2, got: %s", images[0].RepoTags[0])
	}
}

func TestListImagesErrors(t *testing.T) {
	server1, err := dtesting.NewServer("127.0.0.1:0", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server1.Stop()
	server2, err := dtesting.NewServer("127.0.0.1:0", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server2.Stop()
	cluster, err := New(nil, &MapStorage{},
		Node{Address: server1.URL()},
		Node{Address: server2.URL()},
	)
	if err != nil {
		t.Fatal(err)
	}
	opts := docker.PullImageOptions{Repository: "tsuru/python1"}
	err = cluster.PullImage(opts, docker.AuthConfiguration{}, server1.URL())
	if err != nil {
		t.Error(err)
	}
	opts = docker.PullImageOptions{Repository: "tsuru/python2"}
	err = cluster.PullImage(opts, docker.AuthConfiguration{}, server2.URL())
	if err != nil {
		t.Error(err)
	}
	server2.PrepareFailure("list-images-error", "/images/json")
	defer server2.ResetFailure("list-images-error")
	_, err = cluster.ListImages(true)
	if err == nil {
		t.Fatal("Expected error to exist, got <nil>")
	}
}
