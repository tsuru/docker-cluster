// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package storage provides some implementations of the Storage interface,
// defined in the cluster package.
package storage

import (
	"github.com/tsuru/docker-cluster/cluster"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type mongodbStorage struct {
	session *mgo.Session
	dbName  string
}

func (s *mongodbStorage) StoreContainer(container, host string) error {
	coll := s.getColl("containers")
	defer coll.Database.Session.Close()
	_, err := coll.UpsertId(container, bson.M{"$set": bson.M{"host": host}})
	return err
}

func (s *mongodbStorage) RetrieveContainer(container string) (string, error) {
	coll := s.getColl("containers")
	defer coll.Database.Session.Close()
	dbContainer := struct {
		Host string
	}{}
	err := coll.Find(bson.M{"_id": container}).One(&dbContainer)
	if err != nil {
		if err == mgo.ErrNotFound {
			return "", ErrNoSuchContainer
		}
		return "", err
	}
	return dbContainer.Host, nil
}

func (s *mongodbStorage) RemoveContainer(container string) error {
	coll := s.getColl("containers")
	defer coll.Database.Session.Close()
	return coll.Remove(bson.M{"_id": container})
}

func (s *mongodbStorage) StoreImage(image, host string) error {
	coll := s.getColl("images")
	defer coll.Database.Session.Close()
	_, err := coll.UpsertId(image, bson.M{"$push": bson.M{"hosts": host}})
	return err
}

func (s *mongodbStorage) RetrieveImage(image string) ([]string, error) {
	coll := s.getColl("images")
	defer coll.Database.Session.Close()
	dbImage := struct {
		Hosts []string
	}{}
	err := coll.Find(bson.M{"_id": image}).One(&dbImage)
	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, ErrNoSuchImage
		}
		return nil, err
	}
	return dbImage.Hosts, nil
}

func (s *mongodbStorage) RemoveImage(image string) error {
	coll := s.getColl("images")
	defer coll.Database.Session.Close()
	return coll.Remove(bson.M{"_id": image})
}

func (s *mongodbStorage) StoreNode(node cluster.Node) error {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	_, err := coll.UpsertId(node.ID, bson.M{"$set": bson.M{"address": node.Address}})
	return err
}

func (s *mongodbStorage) RetrieveNode(id string) (string, error) {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	var node cluster.Node
	err := coll.Find(bson.M{"_id": id}).One(&node)
	if err != nil {
		if err == mgo.ErrNotFound {
			return "", ErrNoSuchNode
		}
		return "", err
	}
	return node.Address, nil
}

func (s *mongodbStorage) RetrieveNodes() ([]cluster.Node, error) {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	dbNodes := []struct {
		ID      string `bson:"_id"`
		Address string
	}{}
	err := coll.Find(nil).All(&dbNodes)
	if err != nil {
		return nil, err
	}
	nodes := make([]cluster.Node, len(dbNodes))
	for i, node := range dbNodes {
		nodes[i] = cluster.Node{
			ID:      node.ID,
			Address: node.Address,
		}
	}
	return nodes, nil
}

func (s *mongodbStorage) RemoveNode(id string) error {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	return coll.Remove(bson.M{"_id": id})
}

func (s *mongodbStorage) getColl(name string) *mgo.Collection {
	session := s.session.Clone()
	return session.DB(s.dbName).C(name)
}

func Mongodb(addr, dbName string) (cluster.Storage, error) {
	session, err := mgo.Dial(addr)
	if err != nil {
		return nil, err
	}
	storage := mongodbStorage{
		session: session,
		dbName:  dbName,
	}
	return &storage, nil
}
