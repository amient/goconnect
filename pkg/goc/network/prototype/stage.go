package prototype

import "github.com/amient/goconnect/pkg/goc"

type Stage interface {}

type Initialize interface {
	Initialize(node *Node)
}

type Materialize interface {
	Materialize()
}

type RootStage interface {
	Run(collector *goc.Collector)
}

type TransformStage interface {
	Run(input <-chan *goc.Element, collector *goc.Collector)
}

type ElementWiseStage interface {
	Process(input *goc.Element, collector *goc.Collector)
}

