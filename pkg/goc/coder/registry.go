package coder

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder/kv"
	"github.com/amient/goconnect/pkg/goc/coder/str"
	"github.com/amient/goconnect/pkg/goc/coder/url"
	"github.com/amient/goconnect/pkg/goc/coder/xml"
)

func Registry() []goc.Transform {
	return []goc.Transform{
		new(xml.Decoder),
		new(xml.Encoder),
		new(str.Decoder),
		new(str.Encoder),
		new(url.Decoder),
		new(url.Encoder),
		new(kv.NilKeyEncoder),
		new(kv.IgnoreKeyDecoder),
	}
}
