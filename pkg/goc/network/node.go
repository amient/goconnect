package network

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
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
			graph:  make([]*goc.Edge, 0, 20),
		}, nil
	}
}

type Node struct {
	server     *Server
	nodes      []string
	graph      []*goc.Edge
	receiverId int32
}

func (node *Node) GetNodeID() uint16 {
	return node.server.ID
}

func (node *Node) GetNumPeers() uint16 {
	return uint16(len(node.nodes))
}

func (node *Node) GetReceiver(handlerId uint16) goc.Receiver {
	return node.server.NewReceiver(handlerId)
}

func (node *Node) NewSender(targetNodeId uint16, handlerId uint16) goc.Sender {
	addr := node.nodes[targetNodeId - 1]
	sender := newSender(addr, handlerId)
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

func (node *Node) Apply(pipeline *goc.Pipeline) {
	node.graph = make([]*goc.Edge, len(pipeline.Streams))
	log.Printf("Deploying pipline into node %d listening on %v", node.server.ID, node.server.addr)
	for _, stream := range pipeline.Streams {
		dest := goc.NewContext(node, uint16(atomic.AddInt32(&node.receiverId, 1)))
		node.graph[stream.Id] = &goc.Edge{Fn: stream.Fn, Dest: dest}
		if stream.Id > 0 {
			//println(stream.Up.Id, "->", stream.Id )
			node.graph[stream.Id].Source = node.graph[stream.Up.Id].Dest
		}

	}
}

func (node *Node) Run() {
	//FIXME this must terminate only when acks have been processed
	w := sync.WaitGroup{}
	for _, e := range node.graph {
		w.Add(1)
		go func(fn goc.Fn, input <- chan *goc.Element, context *goc.Context) {
			switch stage := fn.(type) {
			case goc.Root:
				stage.Do(context)
			case goc.Transform:
				stage.Run(input, context)
			case goc.ForEach:
				stage.Run(input, context)
			case goc.ElementWiseFn:
				for e := range input {
					stage.Process(e, context)
				}
			case goc.ForEachFn:
				for e := range input {
					stage.Process(e)
				}
			case goc.MapFn:
				for e := range input {
					out := stage.Process(e)
					out.Stamp = e.Stamp
					out.Checkpoint = e.Checkpoint
					context.Emit(out)
				}
			default:
				t := reflect.TypeOf(stage)
				if t.Kind() == reflect.Func && t.NumIn() == 1 && t.NumOut() == 1 {
					//simple mapper function
					v := reflect.ValueOf(stage)
					for e := range input {
						context.Emit(&goc.Element{
							Stamp: e.Stamp,
							Value: v.Call([]reflect.Value{reflect.ValueOf(e.Value)})[0].Interface(),
						})
					}
				} else {
					panic(fmt.Errorf("Unsupported Stage Type %q", t))
				}
			}
			context.Close()
			w.Done()
		}(e.Fn, e.Source.Attach(), e.Dest)
	}
	w.Wait()
	node.server.Close()

}
