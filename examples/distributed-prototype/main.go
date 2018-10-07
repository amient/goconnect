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
		//coder: string -> []uint8
		Apply(new(network.NetRoundRobin)).
		//coder: []uint8 -> string
		Map(func(in string) string { return strings.ToUpper(in) }).Par(2).
		//coder: string -> []uint8
		Apply(new(network.NetMergeOrdered)).
		Apply(new(std.Out))

	network.Runner(pipeline, "127.0.0.1:19001", "127.0.0.1:19002")

}
