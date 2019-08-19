package gzip

import (
	"bytes"
	"compress/gzip"
	"github.com/amient/goconnect"
	"reflect"
)

type Decoder struct{}

func (d *Decoder) InType() reflect.Type {
	return goconnect.BinaryType
}

func (d *Decoder) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (d *Decoder)  Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		buf := new(bytes.Buffer)
		if reader, err := gzip.NewReader(bytes.NewBuffer(input.([]byte))); err != nil {
			panic(err)
		} else if _, err := buf.ReadFrom(reader); err != nil {
			panic(err)
		} else {
			reader.Close()
		}

		return buf.Bytes()
	}
}
