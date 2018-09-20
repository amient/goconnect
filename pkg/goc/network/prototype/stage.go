package prototype

import "github.com/amient/goconnect/pkg/goc"

type Stage interface {
	Run(input <-chan *goc.Element, collector *Collector)
	Materialize()
}

type voidStage struct{}

func NewVoid() Stage {
	return &voidStage{}
}
func (v *voidStage) Run(input <-chan *goc.Element, collector *Collector) {}
func (v *voidStage) Materialize()                                        {}
