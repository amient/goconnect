package network

import (
	"github.com/amient/goconnect/pkg/goc"
)


type NetMergeOrdered struct {
	send            *Sender
	recv            *Receiver
	mergeOnThisNode bool
}

func (n *NetMergeOrdered) Initialize(node *Node) {
	LastNodeID := node.NumPeers()
	n.mergeOnThisNode = node.GetNodeID() == LastNodeID
	targetNode := node.GetPeer(LastNodeID)
	n.send = NewSender(targetNode, node.GetAllocatedReceiverID())
	if n.mergeOnThisNode {
		//log.Printf("merge send --FROM-- %v --TO-- %v", node.server.targetNode, targetNode)
		n.recv = node.GetReceiver("merge")
	}
	n.send.Start()
}

func (n *NetMergeOrdered) Run(input <-chan *goc.Element, context *goc.Context) {
	go func() {
		for e := range input {
			n.send.SendDown(e)
		}
		n.send.Close()
	}()

	if n.mergeOnThisNode {
		buf := goc.NewOrderedElementSet(10)
		for e := range n.recv.Down() {
			buf.AddElement(e, context)
		}
	}

}
