package network

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"io"
	"log"
	"reflect"
	"sync"
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
			graph:  make([]*Edge, 0, 20),
		}, nil
	}
}

//TODO get rid of Initialize by adding all required methods to Context
type Initialize interface {
	Initialize(*Node)
}

type Node struct {
	server     *Server
	nodes      []string
	graph      []*Edge
	receiverId int32
	autoi      uint64
}

type Edge struct {
	src     <-chan *goc.Element
	stage   *goc.Fn
	context *goc.Context
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

func (node *Node) GetReceiver(info string) *Receiver {
	return node.server.NewReceiver(uint16(node.receiverId), info)
}

func (node *Node) GetAllocatedReceiverID() uint16 {
	return uint16(node.receiverId)
}

func (node *Node) Join(nodes []string) {
	for nodeId := 0; nodeId < len(nodes); {
		addr := nodes[nodeId]
		s := NewSender(addr, 0)
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

func (node *Node) Apply(up *goc.Collection, stage goc.Fn) *goc.Collection {

	context := goc.NewContext(node.GetNodeID())
	var upstream <-chan *goc.Element
	if up != nil {
		upstream = up.Elements()
	}
	node.graph = append(node.graph, &Edge{upstream, &stage, context})
	node.allocateNewReceiverID()
	if s, is := stage.(Initialize); is {
		s.Initialize(node)
	}
	return goc.NewCollection(context)
}

func (node *Node) Run() {
	stages := sync.WaitGroup{}
	for _, edge := range node.graph {
		stages.Add(1)
		go func(edge *Edge) {
			c := edge.context
			stage := *edge.stage
			switch stage := stage.(type) {
			case goc.RootFn:
				stage.Do(c)
			case goc.NetworkFn:
				stage.Run(edge.src, c)
			case goc.TransformFn:
				stage.Run(edge.src, c)
			case goc.ElementWiseFn:
				for e := range edge.src {
					stage.Process(e, c)
				}
			case goc.ForEachFn:
				for e := range edge.src {
					stage.Process(e)
				}
			case goc.MapFn:
				for e := range edge.src {
					out := stage.Process(e)
					out.Stamp = e.Stamp
					out.Checkpoint = e.Checkpoint
					c.Emit(out)
				}
			default:
				t := reflect.TypeOf(stage)
				if t.Kind() == reflect.Func && t.NumIn() == 1 && t.NumOut() == 1 {
					//simple mapper function
					v := reflect.ValueOf(stage)
					for e := range edge.src {
						c.Emit(&goc.Element{
							Stamp: e.Stamp,
							Value:  v.Call([]reflect.Value{reflect.ValueOf(e.Value)})[0].Interface(),
						})
					}
				} else {
					panic(fmt.Errorf("Unsupported Stage Type %q", t))
				}

			}
			if cl, is := stage.(io.Closer); is {
				cl.Close()
			}
			c.Close()
			stages.Done()
		}(edge)
	}
	stages.Wait()
	node.server.Close()
}
