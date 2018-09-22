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
	peers := context.GetPeers()
	LastNodeID := uint16(len(peers))
	mergeOnThisNode := context.NodeID == LastNodeID
	var recv goc.Receiver
	if mergeOnThisNode {
		//log.Printf("merge send --FROM-- %v --TO-- %v", node.server.targetNode, targetNode)
		recv = context.GetReceiver()
	}
	targetNode := peers[LastNodeID - 1]
	send := context.GetSender(targetNode)

	go func() {
		for e := range input {
			send.SendDown(e)
		}
		send.Close()
	}()

	if mergeOnThisNode {
		buf := goc.NewOrderedElementSet(10)
		for e := range recv.Down() {
			buf.AddElement(e, context)
		}
	}

}
