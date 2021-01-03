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

package main

import (
	"flag"
	avrolib "github.com/amient/avro"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/coder"
	"github.com/amient/goconnect/coder/serde"
	"github.com/amient/goconnect/io"
	"github.com/amient/goconnect/io/kafka1"
	"time"
)


var (
	kafkaBootstrap    = flag.String("kafka-bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	kafkaCaCert       = flag.String("kafka-ca-cert", "", "Destination Kafka CA Certificate")
	kafkaTopic        = flag.String("kafka-topic", "test_avro", "Destination Kafka Topic")
	kafkaUsername     = flag.String("kafka-username", "", "Destination Kafka Principal")
	kafkaPassword     = flag.String("kafka-password", "", "Destination Kafka Password")
	schemaRegistryUrl = flag.String("schema-registry-url", "http://localhost:8081", "Destination Schema Registry")

	schema, err = avrolib.ParseSchema(`{
  "type": "record",
  "name": "Example",
  "fields": [
    {
      "name": "seqNo",
      "type": "long",
      "default": 0
    },
    {
      "name": "timestamp",
      "type": "long",
      "default": -1
    },
	{
      "name": "text",
      "type": "string",
      "default": ""
	}
  ]}`)
)

type Example struct {
	seqNo     uint64
	timestamp uint64
}

func main() {

	flag.Parse()

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	r1 := avrolib.NewGenericRecord(schema)
	r1.Set("seqNo", int64(1000))
	r1.Set("timestamp", int64(19834720000))
	r1.Set("text", "zG9SwKQCsNgfSD8PDudQoDJenm2bZyOxfhYlOvwUeqFk14eTtT5AZoh4A4UBOcrg3RNeIzzpqbVJ1lnLFuNxBSRXSNkwDjputkBV3gS8eHSThauqri5nZriQrL15z2Hs1bsasKhI6enY9TjT20cyBXe4JjNUE8x4ArQxCpw9I7COE0PUa5b58MOvvXqg3oyj1tw2wIeJHrAUujA7Gv62YuT9KjmJDWMyEYfx4Fv6pUocDh2beLow03ZcjRDpVCSxi9TxUMeHLMvnSwOE3bjVwynQ5dRbFNx88e2EWzWffj2eMToG9ZIfaZEiE6F1tTOhNJuk6b9tI8vppQU7djWA1GSKSorH3zJWguylUmixmBUmPWmEgU2D74oinZ5ALKMjlwGQB4EFjBtmtoD8YH4hi7s051t3uIhK7f8GBHYfNN5ZcYRZ3jt2c8uLvxJ5hwWyAU6NM13gKwwRUXG80MTwZAU6GGhVOoTO8J7PXUP55iX7TKJRbzX5XVys3wPULGJlSkEvLjkgUsOCAfWpw9I2Do1zH3Bm4sqFcoPB3SUQ9ys4TiaqWQMAoym0NHMvYDECugJcrAHTjYHf7EqqBg5O20JbCiYLz9yurxB7QWaJTPcI4oXeCzmt7shb6A4t3M9Z3V078QJtZ2OzOFcmsoeKhI5Pciza38eR7utQJGLD5RSPL4sX5K5nI7TkxAPU1KGH3jIecyFf3VRvYiKKb0qDHltJk9ryiKxUOKzDV8ZNAHFfdmpu8fyOERUVHMzrC1b0ooqLV0nbBTeanDyd2cxdtEQlFAB4VtygJemw7ImhpXnNoqhCB9i0ifV1hidMh2lbgiqOxpjYY2QWXmgoH6zQccvRwX0jHwGy6Ya7ILIiPJ04L3UeDXhqpE18xNrasx3aIZvwWcvfY8Rx6kgArmnyUpJxbrxWOg5DFbgyfVIEYUzDZ9Ovk5MpNfZxdai1c8n6YiwCCv2fuCmwDfRC6SMzDSQh7ZmWYFQfNId1jgI76dXMVykaLzCzzIrkD76KoSST")

	r2 := avrolib.NewGenericRecord(schema)
	r2.Set("seqNo", int64(2000))
	r2.Set("timestamp", int64(19834723000))
	r1.Set("text", "6zpYzrocTdz1e6irxgK1uASJJL83mNiIZEhYdTUU7lOJvKBOHuyH5XBtVrmYWkKLZPPPmdWKFqvOSddL3BdKnTGzo9Vn0H773PMNHcXppoLjdRgTPurId3JTD4jD49erk2NoDhZGdZHZ1SZOnOLfeE0Er2QAbDmoOBgwCV9gkVSFZEfOMxm9JO6dKvCElzNbP6F5S2y1UiHThAZrwlaoaZ3MpJ1r63iRrnayBtHJZ8zspXlsWlgN2E9mWxTF07qyrTd4hXsJ6sxZ37MZGrRSr0s45iSGrGtt6l1F05nNpMZWz4ZWrBkh5G9v9cA7gyYw8403woWBVY1jImYQ50bGNQwKlK5vcdp59wtD5FQZfx0LkDpLwZvdSq9YyUbsrPnVqPin9e9mrfLe7u556a579GY6XoYych0jm83SQn3pUV0ZlvlMSRgnrubX3O7PSEAjcxdS7rLWmHpspfxk9aKMtAgxtrSuf8rOAl7z0PiVwZHq5NX9VR54ICRTULzybE0ZocOEKE9K3kbL3GPcuwpBFNCbpJG09af0OwKlq4W8NdZNSLOlWenLgkWXeCjqZ31o8SdQYuXdtAQGdBngV3tnmDbfuhOT5qiGAZUnlSzvKwA8xyzHpNbzhCOXDVTyF6zNbBtYQZ7JgAWvBZkwafPxYxj6xmxkXMydxNa4jirtXaLh3uk97Y1leMzYHuURGMf6ov7qtlGpaYPeDBixGzhE2ukDSOAgIrMhZ0XJnHkYg2WDviRq8a3mpIe6A2MCDUissoZn7OpcnZbPFX7W3Eh7ohGAnr4Ue3x86JeL3rKODKTkijyhn2KtI7p5O4WVhygyXndgmZhTmhFNgBZFXhBcyN4YTNqBvxDsYWFQzAPMaaef5EVhpl1yoPGv5VMiVoAYcUysGrRmSBdlNdtzcWfNvxPOATORpuNMdfNCF0n455sQ3lp44pqOeeNAkYYFJJ8PcbmWWr8YzRlrgrYFSot6RY7ANNnfptqeclZ6RK1IQpSiU3vhPa2lJxxT9KfxJyl2")

	pipeline.
		Root(io.RoundRobin(10000000, []*avrolib.GenericRecord{r1, r2})).
		Throttle(10, time.Second).
		Apply(&serde.SchemaRegistryEncoder{
			Url: *schemaRegistryUrl,
			Subject: *kafkaTopic + "-value",
			CaCertFile: *kafkaCaCert,
		}).
		Apply(&kafka1.Sink{
			Topic: *kafkaTopic,
			ProducerConfig: kafka1.ConfigMap{
				"bootstrap.servers": *kafkaBootstrap,
				"security.protocol": "SSL",
				//"sasl.mechanisms":   "PLAIN",
				//"sasl.username":     *kafkaUsername,
				//"sasl.password":     *kafkaPassword,
				//"linger.ms":         1000, //FIXME this reduces the throughput ??
				"ssl.ca.location":   *kafkaCaCert,
				"compression.type":  "none",
				//"debug": 			"protocol,cgrp,broker",
			}})
	pipeline.Run()

}
