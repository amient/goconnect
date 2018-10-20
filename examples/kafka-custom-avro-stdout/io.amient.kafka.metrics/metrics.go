package io_amient_kafka_metrics

import "github.com/amient/avro"

var MeasurementSchemaV1, _ = avro.ParseSchema(`{
  "type": "record",
  "name": "MeasurementV1",
  "namespace": "io.amient.kafka.metrics",
  "fields": [
    {
        "name": "timestamp",
        "type": "long"
    },
    {
        "name": "name",
        "type": "string"
    },
    {
        "name": "tags",
        "type": {"type" : "map", "values" : "string"}
    },
    {
        "name": "fields",
        "type": {"type" : "map", "values" : "double"}
    }
  ]
}`)
