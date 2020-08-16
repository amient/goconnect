package coder

import (
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/coder/kv"
	"github.com/amient/goconnect/coder/str"
	"github.com/amient/goconnect/coder/url"
	"github.com/amient/goconnect/coder/xml"
)

func Registry() []goconnect.Transform {
	return []goconnect.Transform{
		new(xml.Decoder),
		new(xml.Encoder),
		new(str.Decoder),
		new(str.Encoder),
		new(url.Decoder),
		new(url.Encoder),
		new(kv.NilKeyEncoder),
		new(kv.NoMetaEncoder),
		new(kv.IgnoreKeyDecoder),
		new(kv.NoMetaDecoder),
	}
}
