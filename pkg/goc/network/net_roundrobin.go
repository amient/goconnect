package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

type NetRoundRobin struct {}

func (n *NetRoundRobin) InType() reflect.Type {
	return goc.ByteArrayType
}

func (n *NetRoundRobin) OutType() reflect.Type {
	return goc.ByteArrayType
}

func (n *NetRoundRobin) Run(input <-chan *goc.Element, context *goc.Context) {
	recv := context.GetReceiver()
	senders := context.GetSenders()
	go func() {
		i := 0
		for e := range input {
			senders[i].Send(e)
			i = (i + 1) % len(senders)
		}
		for _, s := range senders {
			s.Close()
		}
	}()

	for e := range recv.Elements() {
		context.Emit(e)
	}
}

