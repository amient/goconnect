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

package xml

import (
	"bufio"
	"bytes"
	"github.com/amient/goconnect"
	"reflect"
)

type Encoder struct{}

func (d *Encoder) InType() reflect.Type {
	return NodeType
}

func (d *Encoder) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (d *Encoder)  Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		var b bytes.Buffer
		w := bufio.NewWriter(&b)
		_, err := WriteNode(w, input.(Node))
		w.Flush()
		if err != nil {
			panic(err)
		} else {
			return b.Bytes()
		}
	}
}
