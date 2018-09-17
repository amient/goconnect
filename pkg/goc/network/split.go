package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)


func NetSplit() goc.MapFn {
	n := netSplit{
		//server:
	}
	return &n
}

type netSplit struct {}

func (d *netSplit) InType() reflect.Type {
	return goc.ByteArrayType
}

func (d *netSplit) OutType() reflect.Type {
	return goc.ByteArrayType
}

func (d *netSplit) Process(input *goc.Element) *goc.Element {
	return &goc.Element{Value: string(input.Value.([]byte))}
}

