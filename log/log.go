// Copyright 2014 docker-cluster authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package log

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

var (
	logger *log.Logger
	lock   sync.Mutex
	debug  bool
)

func init() {
	SetOutput(nil)
}

func SetOutput(w io.Writer) {
	lock.Lock()
	defer lock.Unlock()
	if w == nil {
		w = os.Stderr
	}
	logger = log.New(w, "", log.LstdFlags)
}

func SetDebug(d bool) {
	debug = d
}

func Debugf(format string, args ...interface{}) {
	lock.Lock()
	defer lock.Unlock()
	if debug {
		logger.Printf(fmt.Sprintf("[docker-cluster][debug] %s", format), args...)
	}
}

func Errorf(format string, args ...interface{}) {
	lock.Lock()
	defer lock.Unlock()
	logger.Printf(fmt.Sprintf("[docker-cluster][error] %s", format), args...)
}
