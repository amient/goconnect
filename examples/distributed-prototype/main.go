package main

import (
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/network/prototype"
	"log"
	"sync"
)

func main() {

	//pipeline := goc.NewPipeline(coder.Registry())
	//messages := pipeline.Root(io.From([]string{"aaa", "bbb", "ccc"}))
	//distributed := messages.Apply(new(NetRoundRobin))
	//transformed := distributed.Apply(new(UpperCase))
	//merged := transformed.Apply(new(NetMergeOrdered))
	//merged.Apply(std.StdOutSink())

	nodes := prototype.JoinCluster([]string{"127.0.0.1:19001", "127.0.0.1:19002"})

	//apply pipeline definition
	log.Println("Declaring")
	w := sync.WaitGroup{}
	for _, node := range nodes {
		w.Add(1)
		go func(node *prototype.Node) {
			s1 := node.Apply(nil, &io.SomeRootStage{Data: []string{"aaa", "bbb", "ccc"}})
			s2 := node.Apply(s1, new(io.NetRoundRobin))
			s3 := node.Apply(s2, new(io.UpperCase))
			s4 := node.Apply(s3, new(io.NetMergeOrdered))
			node.Apply(s4, new(io.StdOutSink))
			w.Done()
		}(node)
	}
	w.Wait()

	prototype.RunLocal(nodes)

}

