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
	"reflect"
)

type AvroBinary struct {
	Schema   avro.Schema
	SchemaID uint32
	Data     []byte
}

var AvroBinaryType = reflect.TypeOf(&AvroBinary{})
var GenericRecordType = reflect.TypeOf(&avro.GenericRecord{})


//TODO SpecificDecoder

type GenericDecoder struct{}

func (d *GenericDecoder) InType() reflect.Type {
	return AvroBinaryType
}

func (d *GenericDecoder) OutType() reflect.Type {
	return GenericRecordType
}

func (d *GenericDecoder) Process(input interface{}) interface{} {
	avroBinary := input.(*AvroBinary)
	decodedRecord := avro.NewGenericRecord(avroBinary.Schema)
	reader := avro.NewDatumReader(avroBinary.Schema)
	if err := reader.Read(decodedRecord, avro.NewBinaryDecoder(avroBinary.Data)); err != nil {
		panic(err)
	}
	return decodedRecord
}
