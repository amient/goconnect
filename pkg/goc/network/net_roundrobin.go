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
	peers := context.GetPeers()
	send := make([]goc.Sender, len(peers))
	for i, addr := range peers {
		send[i] = context.GetSender(addr)
	}
	go func() {
		i := 0
		for e := range input {
			send[i].SendDown(e)
			if i += 1; i >= len(send) {
				i = 0
			}
		}
		for _, s := range send {
			s.Close()
		}
	}()

	for e := range recv.Down() {
		context.Emit(e)
	}
}

