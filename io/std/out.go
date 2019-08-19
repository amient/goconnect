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
	"github.com/amient/goconnect"
	"os"
	"reflect"
)

type Out struct {}

func (sink *Out) InType() reflect.Type {
	return goconnect.AnyType
}

func (sink *Out) Process(input *goconnect.Element, ctx *goconnect.Context) {
	if ctx.Get(0) == nil {
		ctx.Put(0,  new([]*goconnect.Element))
		ctx.Put(1,  bufio.NewWriter(os.Stdout))
	}
	buffer := ctx.Get(0).(*[]*goconnect.Element)
	stdout := ctx.Get(1).(*bufio.Writer)
	process(stdout, input.Value)
	*buffer = append(*buffer, input)
}

func (sink *Out) Flush(ctx *goconnect.Context) error {
	var result error
	if ctx.Get(0) != nil {
		buffer := ctx.Get(0).(*[]*goconnect.Element)
		stdout := ctx.Get(1).(*bufio.Writer)
		if len(*buffer) > 0 {
			result = stdout.Flush()
			for _, e := range *buffer {
				e.Ack()
			}
			*buffer = make([]*goconnect.Element, 0, 100)
		}
	}
	return result
}

