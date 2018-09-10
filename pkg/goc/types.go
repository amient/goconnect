package goc

import (
	"fmt"
	"reflect"
)

var AnyType reflect.Type = reflect.TypeOf([]interface{}{}).Elem()
var ErrorType = reflect.TypeOf(fmt.Errorf("{}"))
var StringType = reflect.TypeOf("")

type KV struct {
	Key   interface{}
	Value interface{}
}

type KVBytes struct {
	Key   []byte
	Value []byte
}

