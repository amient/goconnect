package prototype

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/network"
	"log"
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
}

type Edge struct {
	from      chan *goc.Element
	to        *Stage
	collector *Collector
}

func (node *Node) NewReceiverID() uint16 {
	return uint16(atomic.AddInt32(&node.receiverId,1))
}

func (node *Node) Join(nodes []string) {
	for nodeId := 0; nodeId < len(nodes); {
		addr := nodes[nodeId]
		if s, err := network.NewSender(addr, 0).Start(); err != nil {
			time.Sleep(time.Second)
			log.Printf("Waiting for node at %v to join the cluster..", addr)
		} else {
			s.SendNodeIdentify(nodeId, node.server)
			nodeId ++
		}

	}
	<-node.server.Assigned
}

func (node *Node) Apply(up chan *goc.Element, f StageConstructor) chan *goc.Element {
	collector := NewCollector()
	stage := f(node)
	node.graph = append(node.graph, &Edge{up, &stage, collector})
	return collector.down
}

func (node *Node) Materialize() {
	for _, edge := range node.graph {
		(*edge.to).Materialize()
	}
}

func (node *Node) Run() {
	stages := sync.WaitGroup{}
	for _, edge := range node.graph {
		stages.Add(1)
		go func(edge *Edge){
			(*edge.to).Run(edge.from, edge.collector)
			edge.collector.Close()
			stages.Done()
		}(edge)
	}
	stages.Wait()
	node.server.Close()
}

