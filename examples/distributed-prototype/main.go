package main

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/std"
	"github.com/amient/goconnect/pkg/goc/network"
	"strings"
)

func main() {

	pipeline := goc.NewPipeline(coder.Registry())

	pipeline.
		Root(io.From([]string{"aaa", "bbb", "ccc"})).
		//coder
		Apply(new(network.NetRoundRobin)).
		//coder
		Map(func(input string) string { return strings.ToUpper(input) }).
		//coder
		//Apply(new(network.NetMergeOrdered)).
		//coder
		Apply(new(std.Out))

	network.Runner(pipeline, "127.0.0.1:19001")//, "127.0.0.1:19002")

}
