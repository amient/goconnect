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

package gocstring

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

func Decoder() goc.MapFn {
	return &decoder{}
}

type decoder struct{}

func (d *decoder) InType() reflect.Type {
	return goc.ByteArrayType
}

func (d *decoder) OutType() reflect.Type {
	return goc.StringType
}

func (d *decoder) Process(input *goc.Element) *goc.Element {
	return &goc.Element{Value: string(input.Value.([]byte))}
}
