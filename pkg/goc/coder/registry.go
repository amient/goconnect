package coder

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder/str"
	"github.com/amient/goconnect/pkg/goc/coder/xml"
	"github.com/amient/goconnect/pkg/goc/io/kafka1"
)

func Registry() []goc.MapFn {
	return []goc.MapFn {
		new(xml.Decoder),
		new(xml.Encoder),
		new(str.Decoder),
		new(str.Encoder),
		new(kafka1.NilKeyEncoder),
	}
}
