package prototype

import (
	"github.com/amient/goconnect/pkg/goc"
)

type Stage interface {
	Initialize(node *Node)
	Materialize()
	Run(input <-chan *goc.Element, collector *Collector)
}

