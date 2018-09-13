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

package gocxml

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func StringEncoder() goc.MapFn {
	return &stringEncoder{}
}

type stringEncoder struct{}

func (d *stringEncoder) InType() reflect.Type {
	return NodeType
}

func (d *stringEncoder) OutType() reflect.Type {
	return goc.StringType
}

func (d *stringEncoder) Process(input *goc.Element) *goc.Element {
	str, err := WriteNodeAsString(input.Value.(Node))
	if err != nil {
		panic(err)
	} else {
		return &goc.Element{Value: str}
	}
}