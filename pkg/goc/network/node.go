package network

import (
	"github.com/amient/goconnect/pkg/goc"
	"log"
	"sync/atomic"
	"time"
)

func NewNode(addr string, nodes []string) (*Node, error) {
	server := NewServer(addr)
	if err := server.Start(); err != nil {
		return nil, err
	} else {
		return &Node{
			server: server,
			nodes:  nodes,
		}, nil
	}
}

type Node struct {
	server  *Server
	nodes   []string
	stageId int32
}

func (node *Node) GenerateStageID() uint16 {
	return uint16(atomic.AddInt32(&node.stageId, 1))
}

func (node *Node) GetNodeID() uint16 {
	return node.server.ID
}

func (node *Node) GetNumPeers() uint16 {
	return uint16(len(node.nodes))
}

func (node *Node) GetReceiver(stageId uint16) goc.Receiver {
	return node.server.NewReceiver(stageId)
}

func (node *Node) NewSender(targetNodeId uint16, stageId uint16) goc.Sender {
	addr := node.nodes[targetNodeId - 1]
	sender := newSender(addr, stageId)
	if err := sender.Start(); err != nil {
		panic(err)
	}
	return sender
}

func (node *Node) Join(nodes []string) {
	for nodeId := 0; nodeId < len(nodes); {
		addr := nodes[nodeId]
		s := newSender(addr, 0)
		if err := s.Start(); err != nil {
			time.Sleep(time.Second)
			log.Printf("Waiting for node at %v fn join the cluster..", addr)
		} else {
			s.SendNodeIdentify(nodeId, node.server)
			nodeId ++
		}
	}
	<-node.server.Assigned
}

