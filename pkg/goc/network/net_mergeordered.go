package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)


type NetMergeOrdered struct {}

func (n *NetMergeOrdered) InType() reflect.Type {
	return goc.ByteArrayType
}

func (n *NetMergeOrdered) OutType() reflect.Type {
	return goc.ByteArrayType
}

func (n *NetMergeOrdered) Run(input <-chan *goc.Element, context *goc.Context) {

	LastNode := context.GetNumPeers()
	mergeOnThisNode := context.GetNodeID() == LastNode
	var recv goc.Receiver
	if mergeOnThisNode {
		recv = context.GetReceiver()
	}
	send := context.MakeSender(LastNode)

	go func() {
		for e := range input {
			send.Send(e)
		}
		send.Eos()
	}()

	if mergeOnThisNode {
		buf := goc.NewOrderedElementSet(10)
		for e := range recv.Elements() {
			buf.AddElement(e, context)
		}
	}

}
