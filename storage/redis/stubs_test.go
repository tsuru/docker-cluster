// Copyright 2013 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redis

import (
	"errors"
	"os"
	"os/exec"
	"time"
)

type command struct {
	cmd  string
	args []interface{}
}

type fakeConn struct {
	cmds []command
}

func (c *fakeConn) Close() error {
	return nil
}

func (c *fakeConn) Err() error {
	return nil
}

func (c *fakeConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	if cmd != "" {
		c.cmds = append(c.cmds, command{cmd: cmd, args: args})
	}
	return nil, nil
}

func (c *fakeConn) Send(cmd string, args ...interface{}) error {
	return nil
}

func (c *fakeConn) Flush() error {
	return nil
}

func (c *fakeConn) Receive() (interface{}, error) {
	return nil, nil
}

type failingFakeConn struct {
	fakeConn
}

func (c *failingFakeConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return nil, errors.New("I can't do that.")
}

type resultCommandConn struct {
	*fakeConn
	reply        map[string]interface{}
	defaultReply interface{}
}

func (c *resultCommandConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	c.fakeConn.Do(cmd, args...)
	if c.defaultReply != nil {
		return c.defaultReply, nil
	}
	return c.reply[cmd], nil
}

type redisServer struct {
	cmd      *exec.Cmd
	password string
}

func (s *redisServer) start() error {
	if s.cmd == nil {
		args := []string{"--port", "37455", "--appendfsync", "no"}
		if s.password != "" {
			args = append(args, "--requirepass", s.password)
		}
		s.cmd = exec.Command("redis-server", args...)
	}
	err := s.cmd.Start()
	if err != nil {
		return err
	}
	time.Sleep(1e9)
	return nil
}

func (s *redisServer) stop() error {
	return s.cmd.Process.Signal(os.Interrupt)
}

func (s *redisServer) addr() string {
	return "127.0.0.1:37455"
}
