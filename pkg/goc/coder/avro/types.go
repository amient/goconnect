package avro

import (
	"github.com/amient/avro"
	"reflect"
)

type Binary struct {
	Schema avro.Schema
	Data   []byte
}

type KVBinary struct {
	key Binary
	val Binary
}

var BinaryType = reflect.TypeOf(&Binary{})
var KVBinaryType = reflect.TypeOf(&KVBinary{})
var GenericRecordType = reflect.TypeOf(&avro.GenericRecord{})

