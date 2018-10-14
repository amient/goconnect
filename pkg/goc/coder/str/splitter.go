package str

import (
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
	"strings"
)

func Split(separator string) goc.ElementWise {
	return &Splitter{separator: separator}
}

type Splitter struct {
	separator string
}

func (s *Splitter) InType() reflect.Type {
	return goc.StringType
}

func (s *Splitter) OutType() reflect.Type {
	return goc.StringType
}

func (s *Splitter) Process(input *goc.Element, ctx goc.ProcessContext) {
	for _, s := range strings.Split(input.Value.(string), s.separator) {
		ctx.Emit(s)
	}
}
