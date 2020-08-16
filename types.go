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

package goconnect

import (
	"fmt"
	"reflect"
)

var AnyType = reflect.TypeOf([]interface{}{}).Elem()
var BinaryType = reflect.TypeOf([]byte{})
var ErrorType = reflect.TypeOf(fmt.Errorf("{}"))
var StringType = reflect.TypeOf("")
var IntType = reflect.TypeOf(int(0))
var ByteType = reflect.TypeOf(byte(0))
var Int16Type = reflect.TypeOf(int16(0))
var Int32Type = reflect.TypeOf(int32(0))
var Int64Type = reflect.TypeOf(int64(0))

var KVBinaryType = reflect.TypeOf(&KVBinary{})
type KVBinary struct {
	Key   []byte
	Value []byte
}

var KVMBinaryType = reflect.TypeOf(&KVMBinary{})
type KVMBinary struct {
	Key   []byte
	Value []byte
	Headers map[string][]byte
}

