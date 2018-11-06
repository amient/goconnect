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

package avro

import (
	"github.com/amient/avro"
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

//TODO type BinaryEncoder struct {}
//
//func (e *BinaryEncoder) InType() reflect.Type {
//	return GenericRecordType
//}
//
//func (e *BinaryEncoder) OutType() reflect.Type {
//	return goc.BinaryType
//}
//TODO the same encoder should work for both Generic and Specific Records


type JsonEncoder struct{}

func (e *JsonEncoder) InType() reflect.Type {
	return GenericRecordType
}

func (e *JsonEncoder) OutType() reflect.Type {
	return goc.StringType
}

func (e *JsonEncoder) Process(in interface{}) interface{} {
	return in.(*avro.GenericRecord).String()
}
