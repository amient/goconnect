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
	"bytes"
	"encoding/binary"
	"github.com/amient/avro"
	"github.com/amient/goconnect"
	"reflect"
)

type JsonEncoder struct{}

func (e *JsonEncoder) InType() reflect.Type {
	return GenericRecordType
}

func (e *JsonEncoder) OutType() reflect.Type {
	return goconnect.StringType
}

func (e *JsonEncoder) Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		return input.(*avro.GenericRecord).String()
	}
}

type GenericEncoder struct{}

func (e *GenericEncoder) InType() reflect.Type {
	return GenericRecordType
}

func (e *GenericEncoder) OutType() reflect.Type {
	return BinaryType
}

func (e *GenericEncoder) Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		schema := input.(*avro.GenericRecord).Schema()
		writer := avro.NewGenericDatumWriter().SetSchema(schema)
		buf := new(bytes.Buffer)
		if err := writer.Write(input, avro.NewBinaryEncoder(buf)); err != nil {
			panic(err)
		}
		return &Binary{
			Schema: schema,
			Data:   buf.Bytes(),
		}
	}
}

type SchemaRegistryEncoder struct {
	Url            string
	Subject        string
	CaCertFile     string
	ClientCertFile string
	ClientKeyFile  string
	ClientKeyPass  string
}

func (cf *SchemaRegistryEncoder) InType() reflect.Type {
	return BinaryType
}

func (cf *SchemaRegistryEncoder) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (cf *SchemaRegistryEncoder) Materialize() func(input interface{}) interface{} {
	if cf.Subject == "" {
		//TODO if no subject is provided use schema.namespace + . + name
		panic("Subject not defined for SchemaRegistryEncoder")
	}
	client := &avro.SchemaRegistryClient{Url: cf.Url}
	var err error
	client.Tls, err = avro.TlsConfigFromPEM(cf.ClientCertFile, cf.ClientKeyFile, cf.ClientKeyPass, cf.CaCertFile)
	if err != nil {
		panic(err)
	}
	return func(input interface{}) interface{} {
		ab := input.(*Binary)
		schemaId, err := client.GetSchemaId(ab.Schema, cf.Subject)
		if err != nil {
			panic(err)
		}
		buf := new(bytes.Buffer)
		if err := buf.WriteByte(0); err != nil {
			panic(err)
		} else if err := binary.Write(buf, binary.BigEndian, schemaId); err != nil {
			panic(err)
		} else if _, err := buf.Write(ab.Data); err != nil {
			panic(err)
		} else {
			return buf.Bytes()
		}
	}
}

//TODO SpecificEncoder

//TODO KVGenericEncoder (avro.KVBinary > goconnect.KVBinary)

//TODO type SchemaRegistryKVEncoder struct {
//	Url   string
//	Topic string
//	//TODO add ca cert
//	//TODO add ssl cert
//	//TODO add ssl key
//	//TODO add ssl key password
//}
//
//func (cf *SchemaRegistryKVEncoder) InType() reflect.Type {
//	return KVBinaryType
//}
//
//func (cf *SchemaRegistryKVEncoder) OutType() reflect.Type {
//	return goconnect.KVBinaryType
//}
//
//func (cf *SchemaRegistryKVEncoder) Materialize() func(input *goconnect.Element, context goconnect.PContext) {
//	if cf.Topic == "" {
//		panic("Topic not defined for SchemaRegistryKVEncoder")
//	}
//	client := &schemaRegistryClient{url: cf.Url}
//}
