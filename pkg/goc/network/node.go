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

func (node *Node) GetPeers() []string {
	return node.nodes
}

func (node *Node) GetReceiver(handlerId uint16) goc.Receiver {
	return node.server.NewReceiver(handlerId)
}

func (node *Node) NewSender(addr string, handlerId uint16) goc.Sender {
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
		context := goc.NewContext(node.server.ID, node, uint16(atomic.AddInt32(&node.receiverId, 1)))
		dest := goc.NewCollection(context)
		node.graph[stream.Id] = &goc.Edge{Context: context, Fn: stream.Fn, Dest: dest}
		if stream.Id > 0 {
			//println(stream.Up.Id, "->", stream.Id )
			node.graph[stream.Id].Source = node.graph[stream.Up.Id].Dest
		}

	}
}

func (node *Node) Run() {
	//FIXME this must terminate only when acks have been processed
	stages := sync.WaitGroup{}
	for _, e := range node.graph {
		stages.Add(1)
		fn := e.Fn
		go func(fn goc.Fn, source *goc.Collection, context *goc.Context) {
			switch stage := fn.(type) {
			case goc.Root:
				stage.Do(context)
			case goc.Transform:
				stage.Run(source.Elements(), context)
			case goc.ForEach:
				stage.Run(source.Elements(), context)
			case goc.ElementWiseFn:
				for e := range source.Elements() {
					stage.Process(e, context)
				}
			case goc.ForEachFn:
				for e := range source.Elements() {
					stage.Process(e)
				}
			case goc.MapFn:
				for e := range source.Elements() {
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
					for e := range source.Elements() {
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
			stages.Done()
		}(fn, e.Source, e.Context)
	}
	stages.Wait()
	node.server.Close()

}
