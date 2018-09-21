package prototype

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/network"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

func NewNode(addr string, nodes []string) (*Node, error) {
	server := network.NewServer(addr)
	if err := server.Start(); err != nil {
		return nil, err
	} else {
		return &Node{
			server: server,
			nodes:  nodes,
			graph:  make([]*Edge, 0, 20),
		}, nil
	}
}

type StageConstructor func(*Node) Stage

type Node struct {
	server     *network.Server
	nodes      []string
	graph      []*Edge
	receiverId int32
	autoi      uint64
}

type Edge struct {
	src       <-chan *goc.Element
	stage     *Stage
	collector *goc.Collector
}

func (node *Node) GetNodeID() uint16 {
	return node.server.NodeId
}

func (node *Node) NumPeers() uint16 {
	return uint16(len(node.nodes))
}

func (node *Node) GetPeers() []string {
	return node.nodes
}

func (node *Node) GetPeer(nodeId uint16) string {
	return node.nodes[nodeId-1]
}

func (node *Node) allocateNewReceiverID() uint16 {
	return uint16(atomic.AddInt32(&node.receiverId, 1))
}

func (node *Node) GetReceiver(info string) *network.Receiver {
	return node.server.NewReceiver(uint16(node.receiverId), info)
}

func (node *Node) GetAllocatedReceiverID() uint16 {
	return uint16(node.receiverId)
}

func (node *Node) Join(nodes []string) {
	for nodeId := 0; nodeId < len(nodes); {
		addr := nodes[nodeId]
		s := network.NewSender(addr, 0)
		if err := s.Start(); err != nil {
			time.Sleep(time.Second)
			log.Printf("Waiting for node at %v stage join the cluster..", addr)
		} else {
			s.SendNodeIdentify(nodeId, node.server)
			nodeId ++
		}
	}
	<-node.server.Assigned
}

func (node *Node) Apply(up *goc.Collection, stage Stage) *goc.Collection {

	collector := goc.NewCollector(node.GetNodeID())
	var upstream <- chan *goc.Element
	if up != nil {
		upstream = up.Elements()
	}
	node.graph = append(node.graph, &Edge{upstream, &stage, collector})
	node.allocateNewReceiverID()
	if s, is := stage.(Initialize); is {
		s.Initialize(node)
	}
	return goc.NewCollection(collector)
}

func (node *Node) Materialize() {
	for _, edge := range node.graph {
		if s, is := (*edge.stage).(Materialize); is {
			s.Materialize()
		}
	}
}

func (node *Node) Run() {
	stages := sync.WaitGroup{}
	for _, edge := range node.graph {
		stages.Add(1)
		go func(edge *Edge) {
			c := edge.collector
			switch stage := (*edge.stage).(type) {
			case RootStage:
				stage.Run(c)
			case TransformStage:
				stage.Run(edge.src, c)
			case ElementWiseStage:
				for e := range edge.src {
					stage.Process(e, c)
				}
			default:
				panic(fmt.Errorf("Unsupported Stage Type %q", reflect.TypeOf(stage)))
			}
			c.Close()
			stages.Done()
		}(edge)
	}
	stages.Wait()
	node.server.Close()
}
