package io

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/network"
	"github.com/amient/goconnect/pkg/goc/network/prototype"
)

type NetRoundRobin struct {
	instance *prototype.Node
	send     []*network.Sender
	recv     *network.Receiver
}

func (n *NetRoundRobin) Initialize(node *prototype.Node) {
	n.instance = node
	n.recv = node.GetReceiver("round-robin")
	n.send = make([]*network.Sender, node.NumPeers())
	for i, addr := range node.GetPeers() {
		n.send[i] = network.NewSender(addr, n.recv.ID)
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
	send            *network.Sender
	recv            *network.Receiver
	mergeOnThisNode bool
}

func (n *NetMergeOrdered) Initialize(node *prototype.Node) {
	LastNodeID := node.NumPeers()
	n.mergeOnThisNode = node.GetNodeID() == LastNodeID
	targetNode := node.GetPeer(LastNodeID)
	n.send = network.NewSender(targetNode, node.GetAllocatedReceiverID())
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

type StdOutSink struct{}

func (s *StdOutSink) Process(input *goc.Element, collector *goc.Collector) {
	println(string(input.Value.([]byte)), input.Stamp.String())
	//TODO input.Ack()
}

