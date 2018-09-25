package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"reflect"
)

type NetRoundRobin struct{}

func (n *NetRoundRobin) InType() reflect.Type {
	return goc.ByteArrayType
}

func (n *NetRoundRobin) OutType() reflect.Type {
	return goc.ByteArrayType
}

func (n *NetRoundRobin) Run(input <-chan *goc.Element, context *goc.Context) {
	receiver := context.MakeReceiver()
	senders := context.MakeSenders()
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

	for !context.Closed() {
		select {
		case checkpoint, ok := <-context.Commits():
			if ok {
				log.Println("!", checkpoint)
			}
		case e, ok := <-receiver.Elements():
			if ok {

				e.Checkpoint = goc.Checkpoint{
					Part: int(e.Stamp.Trace[len(e.Stamp.Trace)-1]),
					Data: e.Stamp,
				}
				context.Emit(e)
			}

		}
	}
}
