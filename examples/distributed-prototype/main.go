package main

import (
	"github.com/amient/goconnect/pkg/goc"
	"github.com/amient/goconnect/pkg/goc/coder"
	"github.com/amient/goconnect/pkg/goc/io"
	"github.com/amient/goconnect/pkg/goc/io/std"
	"github.com/amient/goconnect/pkg/goc/network"
	"strings"
	"time"
)

func main() {

	pipeline := goc.NewPipeline(coder.Registry())

	pipeline.
		Root(io.From([]string{"aaa", "bbb", "ccc"})).
		Apply(new(network.NetRoundRobin)).
		Apply(func(input string) string { return strings.ToUpper(input) }).
		Apply(new(network.NetMergeOrdered)).
		Apply(new(std.Out))

	network.Runner(pipeline, "127.0.0.1:19001", "127.0.0.1:19002")

	//FIXME
	time.Sleep(1 * time.Second)

}
