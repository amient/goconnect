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

func StdOutSink() goc.ForEachFn {
	return &stdOutSink{
		stdout: bufio.NewWriter(os.Stdout),
	}
}

type stdOutSink struct {
	stdout *bufio.Writer
}

func (sink *stdOutSink) InType() reflect.Type {
	return goc.AnyType
}

func (sink *stdOutSink) Process(input *goc.Element) {
	sink.Fn(input.Value)
}

func (sink *stdOutSink) Fn(element interface{}) {
	switch e := element.(type) {
	case []byte:
		sink.stdout.Write(e)
	case string:
		sink.stdout.WriteString(e)
	case goc.KV:
		sink.Fn(e.Key)
		sink.Fn(" -> ")
		sink.Fn(e.Value)
	case goc.KVBytes:
		sink.Fn(e.Key)
		sink.Fn(" -> ")
		sink.Fn(e.Value)
	default:
		fmt.Fprint(sink.stdout, element)
	}
	sink.stdout.WriteByte('\n')
}

func (sink *stdOutSink) Flush() error {
	return sink.stdout.Flush()
}
