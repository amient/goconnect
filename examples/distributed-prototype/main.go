package main

import (
	"github.com/amient/goconnect/pkg/goc/coder/gocstring"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/std"
	"github.com/amient/goconnect/pkg/goc/network"
	"log"
	"strings"
	"sync"
)

func main() {

	//pipeline := goc.NewPipeline(coder.Registry())
	//messages := pipeline.Root(io.From([]string{"aaa", "bbb", "ccc"}))
	//distributed := messages.Apply(new(network.NetRoundRobin))
	//transformed := distributed.Apply(func(input string) string { return strings.ToUpper(input) })
	//merged := transformed.Apply(new(network.NetMergeOrdered))
	//merged.Apply(std.StdOutSink())

	nodes := network.JoinCluster([]string{"127.0.0.1:19001", "127.0.0.1:19002"})

	//apply pipeline definition
	log.Println("Declaring")
	w := sync.WaitGroup{}
	for _, node := range nodes {
		w.Add(1)
		go func(node *network.Node) {
			s1 := node.Apply(nil, io.From([]string{"aaa", "bbb", "ccc"}))
			s2 := node.Apply(s1, new(gocstring.Encoder))
			s3 := node.Apply(s2, new(network.NetRoundRobin))
			s4 := node.Apply(s3, new(gocstring.Decoder))
			s5 := node.Apply(s4, func(input string) string { return strings.ToUpper(input) })
			s6 := node.Apply(s5, new(gocstring.Encoder))
			s7 := node.Apply(s6, new(network.NetMergeOrdered))
			node.Apply(s7, std.StdOutSink())
			w.Done()
		}(node)
	}
	w.Wait()

	network.RunLocal(nodes)

}


