package network

import (
	"github.com/amient/goconnect/pkg/goc"
)

type NetRoundRobin struct {
	instance *Node
	send     []*Sender
	recv     *Receiver
}

func (n *NetRoundRobin) Initialize(node *Node) {
	n.instance = node
	n.recv = node.GetReceiver("round-robin")
	n.send = make([]*Sender, node.NumPeers())
	for i, addr := range node.GetPeers() {
		n.send[i] = NewSender(addr, n.recv.ID)
	}
	for _, s := range n.send {
		if err := s.Start(); err != nil {
			panic(err)
		}
	}
}

func (n *NetRoundRobin) Run(input <-chan *goc.Element, collector *goc.Collector) {
	go func() {
		i := 0
		for e := range input {
			n.send[i].SendDown(e)
			if i += 1; i >= len(n.send) {
				i = 0
			}
		}
		for _, s := range n.send {
			s.Close()
		}
	}()

	for e := range n.recv.Down() {
		collector.Emit(e)
	}
}

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

func (n *NetMergeOrdered) Run(input <-chan *goc.Element, collector *goc.Collector) {
	go func() {
		for e := range input {
			n.send.SendDown(e)
		}
		n.send.Close()
	}()

	if n.mergeOnThisNode {
		buf := goc.NewOrderedElementSet(10)
		for e := range n.recv.Down() {
			buf.AddElement(e, collector)
		}
	}

}
