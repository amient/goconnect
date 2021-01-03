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
	"encoding/binary"
	schema_registry "github.com/amient/go-schema-registry-client"
	"github.com/amient/goconnect"
	"reflect"
)

type SchemaRegistryDecoder struct {
	Url            string
	CaCertFile     string
	ClientCertFile string
	ClientKeyFile  string
	ClientKeyPass  string
}

func (cf *SchemaRegistryDecoder) InType() reflect.Type {
	return goconnect.BinaryType
}

func (cf *SchemaRegistryDecoder) OutType() reflect.Type {
	return BinaryType
}

func (cf *SchemaRegistryDecoder) Materialize() func(input interface{}) interface{} {
	cfg := &schema_registry.Config{Url: cf.Url}
	if cf.CaCertFile != "" {
		err := cfg.AddCaCert(cf.CaCertFile)
		if err != nil {
			panic(err)
		}
	}
	if cf.ClientCertFile != "" {
		err := cfg.AddClientCert(cf.ClientCertFile, cf.ClientKeyFile, cf.ClientKeyPass)
		if err != nil {
			panic(err)
		}
	}
	//client := &avro.SchemaRegistryClient{Url: cf.Url}
	client2 := schema_registry.NewClientWith(cfg)
	//client.Tls = tlsConfig
	ctx := context.Background() //TODO this should be passed to Materialize(ctx)

	return func(input interface{}) interface{} {
		bytes := input.([]byte)
		switch bytes[0] {
		case 0:
			schemaId := binary.BigEndian.Uint32(bytes[1:])
			schema, err := client2.GetSchema(ctx, schemaId)
			if err != nil {
				panic(err)
			}
			return &Binary{
				Client: client2,
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

func (d *GenericDecoder) Materialize() func(input interface{}) interface{} {
	ctx := context.Background()
	return func(input interface{}) interface{} {
		binary := input.(*Binary)
		record, err := binary.Client.Deserialize(ctx, binary.Data)
		if err != nil {
			panic(err)
		}
		return record
	}
}
