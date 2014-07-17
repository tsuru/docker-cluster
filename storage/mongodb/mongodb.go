// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mongodb

import (
	"github.com/tsuru/docker-cluster/cluster"
	"github.com/tsuru/docker-cluster/storage"
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
			return "", storage.ErrNoSuchContainer
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
	_, err := coll.UpsertId(image, bson.M{"$addToSet": bson.M{"hosts": host}})
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
			return nil, storage.ErrNoSuchImage
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

type dbNode struct {
	Address  string `bson:"_id"`
	Metadata map[string]string
}

func toClusterNode(dbNodes []dbNode) []cluster.Node {
	nodes := make([]cluster.Node, len(dbNodes))
	for i, node := range dbNodes {
		nodes[i] = cluster.Node{
			Address:  node.Address,
			Metadata: node.Metadata,
		}
	}
	return nodes
}

func (s *mongodbStorage) StoreNode(node cluster.Node) error {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	err := coll.Insert(dbNode{Address: node.Address, Metadata: node.Metadata})
	if mgo.IsDup(err) {
		return storage.ErrDuplicatedNodeAddress
	}
	return err
}

func (s *mongodbStorage) RetrieveNodesByMetadata(metadata map[string]string) ([]cluster.Node, error) {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	query := bson.M{}
	for key, value := range metadata {
		query["metadata."+key] = value
	}
	var dbNodes []dbNode
	err := coll.Find(query).All(&dbNodes)
	if err != nil {
		return nil, err
	}
	return toClusterNode(dbNodes), nil
}

func (s *mongodbStorage) RetrieveNodes() ([]cluster.Node, error) {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	var dbNodes []dbNode
	err := coll.Find(nil).All(&dbNodes)
	if err != nil {
		return nil, err
	}
	return toClusterNode(dbNodes), nil
}

func (s *mongodbStorage) RetrieveNode(address string) (cluster.Node, error) {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	var node dbNode
	err := coll.FindId(address).One(&node)
	if err == mgo.ErrNotFound {
		return cluster.Node{}, storage.ErrNoSuchNode
	}
	return cluster.Node{Address: node.Address, Metadata: node.Metadata}, err
}

func (s *mongodbStorage) UpdateNode(node cluster.Node) error {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	err := coll.UpdateId(node.Address, dbNode{Address: node.Address, Metadata: node.Metadata})
	if err == mgo.ErrNotFound {
		return storage.ErrNoSuchNode
	}
	return err
}

func (s *mongodbStorage) RemoveNode(address string) error {
	coll := s.getColl("nodes")
	defer coll.Database.Session.Close()
	err := coll.Remove(bson.M{"_id": address})
	if err == mgo.ErrNotFound {
		return storage.ErrNoSuchNode
	}
	return err
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
