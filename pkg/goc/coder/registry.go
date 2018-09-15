package coder

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder/gocstring"
	"github.com/amient/goconnect/pkg/goc/coder/gocxml"
	"github.com/amient/goconnect/pkg/goc/io/kafka1"
)

func Registry() []goc.MapFn {
	return []goc.MapFn {
		new(gocxml.Decoder),
		new(gocxml.Encoder),
		new(gocstring.Decoder),
		new(gocstring.Encoder),
		new(kafka1.NilKeyEncoder),
	}
}
