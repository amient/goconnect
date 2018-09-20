package prototype

import "github.com/amient/goconnect/pkg/goc"

type Stage interface {
	Initialize(node *Node)
	Materialize()
}

type RootStage interface {
	Run(collector *Collector)
}

type TransformStage interface {
	Run(input <-chan *goc.Element, collector *Collector)
}

type ElementWiseStage interface {
	Process(input *goc.Element, collector *Collector)
}
