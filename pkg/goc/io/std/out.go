/*
 * Copyright 2018 Amient Ltd, London
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package std

import (
	"bufio"
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"os"
	"reflect"
	"time"
)

func StdOutSink() goc.ForEachFn {
	sink := stdOutSink{
		buffer: make([]*goc.Element, 0, 100),
		queue:  make(chan *goc.Element),
		closed: make(chan error, 1),
		stdout: bufio.NewWriter(os.Stdout),
	}
	//FIXME this type of initialization has to happen during materialization not initialization
	ticker := time.NewTicker(300 * time.Millisecond).C
	go func() {
		for {
			select {
			case <-ticker:
				sink.flush()
			case e := <-sink.queue:
				sink.buffer = append(sink.buffer, e)
			case <-sink.closed:
				sink.closed <- sink.flush()
				return
			}
		}
	}()
	return &sink
}

type stdOutSink struct {
	queue  chan *goc.Element
	buffer []*goc.Element
	closed chan error
	stdout *bufio.Writer
}

func (sink *stdOutSink) InType() reflect.Type {
	return goc.AnyType
}

func (sink *stdOutSink) Process(input *goc.Element) {
	sink.process(input.Value)
	sink.queue <- input
}

func (sink *stdOutSink) flush() error {
	for _, e := range sink.buffer {
		e.Ack()
	}
	sink.buffer = make([]*goc.Element, 0, 100)
	return sink.stdout.Flush()
}

func (sink *stdOutSink) process(element interface{}) {
	switch e := element.(type) {
	case []byte:
		sink.stdout.Write(e)
	case string:
		sink.stdout.WriteString(e)
	case goc.KV:
		sink.process(e.Key)
		sink.process(" -> ")
		sink.process(e.Value)
	case goc.KVBytes:
		sink.process(e.Key)
		sink.process(" -> ")
		sink.process(e.Value)
	default:
		fmt.Fprint(sink.stdout, element)
	}
	sink.stdout.WriteByte('\n')
}

func (sink *stdOutSink) Close() error {
	sink.flush()
	sink.closed <- nil
	return <-sink.closed
}
