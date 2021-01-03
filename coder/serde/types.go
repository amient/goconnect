package serde

import (
	schema_registry "github.com/amient/go-schema-registry-client"
	"reflect"
)

type Binary struct {
	Client *schema_registry.Client
	Schema schema_registry.Schema
	Data   []byte
}

type KVBinary struct {
	key Binary
	val Binary
}

var BinaryType = reflect.TypeOf(&Binary{})
var KVBinaryType = reflect.TypeOf(&KVBinary{})
var GenericRecordType = reflect.TypeOf((*interface{})(nil)).Elem()
