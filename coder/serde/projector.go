package serde

import (
	"context"
	"reflect"
)

type GenericProjector struct {
	TargetType reflect.Type
}

func (p *GenericProjector) InType() reflect.Type {
	return BinaryType
}

func (p *GenericProjector) OutType() reflect.Type {
	return GenericRecordType
}

func (p *GenericProjector) Materialize() func(input interface{}) interface{} {
	//projections := make(map[avrolib.Fingerprint]avrolib.DatumReader)
	ctx := context.Background()
	return func(input interface{}) interface{} {
		inputBinary := input.(*Binary)
		result := reflect.New(p.TargetType)
		err := inputBinary.Client.DeserializeInto(ctx, inputBinary.Data, result)
		if err != nil {
			panic(err)
		}
		return result
	}
}

