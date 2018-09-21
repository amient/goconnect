package network

import "github.com/amient/goconnect/pkg/goc"

type Stage interface {}

type Initialize interface {
	Initialize(*Node)
}

type RootStage interface {
	Do(*goc.Context)
}

type TransformStage interface {
	Run(<-chan *goc.Element, *goc.Context)
}

type ElementWiseStage interface {
	Process(*goc.Element, *goc.Context)
}

type ForEachStage interface {
	Process(*goc.Element)
}
