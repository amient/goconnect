package gzip

import (
	"bytes"
	"compress/gzip"
	"github.com/amient/goconnect"
	"reflect"
)

type Encoder struct{}

func (d *Encoder) InType() reflect.Type {
	return goconnect.BinaryType
}

func (d *Encoder) OutType() reflect.Type {
	return goconnect.BinaryType
}

func (d *Encoder)  Materialize() func(input interface{}) interface{} {
	return func(input interface{}) interface{} {
		buf := new(bytes.Buffer)
		gz := gzip.NewWriter(buf)
		if _, err := gz.Write(input.([]byte)); err != nil {
			panic(err)
		} else if err = gz.Flush(); err != nil {
			panic(err)
		} else {
			gz.Close()
		}
		return buf.Bytes()
	}
}
