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
	messages := pipeline.Root(io.From([]string{"aaa", "bbb", "ccc"}))
	distributed := messages.Apply(new(network.NetRoundRobin))
	transformed := distributed.Apply(func(input string) string { return strings.ToUpper(input) })
	merged := transformed.Apply(new(network.NetMergeOrdered))
	merged.Apply(new(std.Out))

	network.Runner(pipeline, "127.0.0.1:19001", "127.0.0.1:19002")

}

