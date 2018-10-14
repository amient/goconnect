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
)

type Out struct {
	buffer []*goc.Element
	stdout *bufio.Writer
}

func (sink *Out) InType() reflect.Type {
	return goc.AnyType
}

func (sink *Out) Process(input *goc.Element, ctx *goc.Context) {
	if sink.stdout == nil {
		sink.stdout = bufio.NewWriter(os.Stdout)
	}
	sink.process(sink.stdout, input.Value)
	sink.buffer = append(sink.buffer, input)
}

func (sink *Out) Flush(ctx *goc.Context) error {
	var result error
	if len(sink.buffer) > 0 {
		result = sink.stdout.Flush()
		for _, e := range sink.buffer {
			e.Ack()
		}
		sink.buffer = make([]*goc.Element, 0, 100)
	}
	return result
}

func (sink *Out) process(stdout *bufio.Writer, element interface{}) {
	switch e := element.(type) {
	case []byte:
		stdout.Write(e)
	case string:
		stdout.WriteString(e)
	case goc.KV:
		sink.process(stdout, e.Key)
		sink.process(stdout, " -> ")
		sink.process(stdout, e.Value)
	case goc.KVBytes:
		sink.process(stdout, e.Key)
		sink.process(stdout, " -> ")
		sink.process(stdout, e.Value)
	default:
		fmt.Fprint(stdout, element)
	}
	stdout.WriteByte('\n')
}
