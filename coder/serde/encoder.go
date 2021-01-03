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

package serde

import (
	"context"
	"encoding/json"
	"github.com/amient/avro"
	schema_registry "github.com/amient/go-schema-registry-client"
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
		j, err := json.Marshal(input)
		if err != nil {
			panic(err)
		}
		return string(j)
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
	return GenericRecordType
}

func (cf *SchemaRegistryEncoder) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (cf *SchemaRegistryEncoder) Materialize() func(input interface{}) interface{} {
	tlsConfig, err := avro.TlsConfigFromPEM(cf.ClientCertFile, cf.ClientKeyFile, cf.ClientKeyPass, cf.CaCertFile)
	if err != nil {
		panic(err)
	}
	if cf.Subject == "" {
		panic("Subject not defined for SchemaRegistryEncoder")
	}
	client := schema_registry.NewClientWith(&schema_registry.Config{
		Url: cf.Url,
		Tls: tlsConfig,
		LogLevel: schema_registry.LogEverything,
	})
	ctx := context.Background()
	return func(input interface{}) interface{} {
		data, err := client.Serialize(ctx, cf.Subject, input)
		if err != nil {
			panic(err)
		}
		return data
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
