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

package file

import (
	"bytes"
	"github.com/amient/goconnect"
	"reflect"
)

type Text struct {
	//TODO text must have an encoding and the reading of the input ByteStream needs to recognize line character in that encoding
}

func (r *Text) InType() reflect.Type {
	return ByteStreamType
}

func (r *Text) OutType() reflect.Type {
	return goconnect.StringType
}

func (r *Text) Materialize() func(input *goconnect.Element, context goconnect.PContext) {
	return func(input *goconnect.Element, ctx goconnect.PContext) {
		bs := input.Value.(ByteStream)
		buf := new(bytes.Buffer)
		b := make([]byte, 1)
		emit := func() {
			if buf.Len() > 0 {
				ctx.Emit(&goconnect.Element{Value: string(buf.Bytes())})
				buf.Reset()
			}
		}
		for bs.Read(&b) > 0 {
			if b[0] == '\n' {
				emit()
			} else {
				buf.Write(b)
			}
		}
		emit()
	}
}
