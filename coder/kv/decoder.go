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

package kv

import (
	"github.com/amient/goconnect"
	"reflect"
)

type IgnoreKeyDecoder struct{}

func (d *IgnoreKeyDecoder) InType() reflect.Type {
	return goconnect.KVBinaryType
}

func (d *IgnoreKeyDecoder) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (d *IgnoreKeyDecoder) Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		return input.(*goconnect.KVBinary).Value
	}
}

type NoMetaDecoder struct{}

func (d *NoMetaDecoder) InType() reflect.Type {
	return goconnect.KVMBinaryType
}

func (d *NoMetaDecoder) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (d *NoMetaDecoder) Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		return input.(*goconnect.KVMBinary).Value
	}
}