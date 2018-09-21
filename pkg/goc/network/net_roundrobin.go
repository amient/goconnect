package network

import "github.com/amient/goconnect/pkg/goc"

type NetRoundRobin struct {
	send     []*Sender
	recv     *Receiver
}

func (n *NetRoundRobin) Initialize(node *Node) {
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

