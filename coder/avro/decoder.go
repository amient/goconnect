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
	"encoding/binary"
	"github.com/amient/avro"
	"github.com/amient/goconnect"
	"reflect"
)

type SchemaRegistryDecoder struct {
	Url string
	//TODO add ca cert
	//TODO add ssl cert
	//TODO add ssl key
	//TODO add ssl key password
}

func (cf *SchemaRegistryDecoder) InType() reflect.Type {
	return goconnect.BinaryType
}

func (cf *SchemaRegistryDecoder) OutType() reflect.Type {
	return BinaryType
}

func (cf *SchemaRegistryDecoder) Materialize() func(input interface{}) interface{} {
	client := &avro.SchemaRegistryClient{Url: cf.Url}
	return func(input interface{}) interface{} {
		bytes := input.([]byte)
		switch bytes[0] {
		case 0:
			schemaId := binary.BigEndian.Uint32(bytes[1:])
			schema := client.Get(schemaId)
			return &Binary{
				Schema: schema,
				Data:   bytes[5:],
			}
		default:
			panic("avro binary header incorrect")
		}
	}
}

type GenericDecoder struct{}

func (d *GenericDecoder) InType() reflect.Type {
	return BinaryType
}

func (d *GenericDecoder) OutType() reflect.Type {
	return GenericRecordType
}

func (d *GenericDecoder)  Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		avroBinary := input.(*Binary)
		decodedRecord := avro.NewGenericRecord(avroBinary.Schema)
		reader := avro.NewDatumReader(avroBinary.Schema)
		if err := reader.Read(decodedRecord, avro.NewBinaryDecoder(avroBinary.Data)); err != nil {
			panic(err)
		}
		return decodedRecord
	}
}

//TODO KVGenericDecoder

//TODO SpecificDecoder

//TODO JsonDecoder
