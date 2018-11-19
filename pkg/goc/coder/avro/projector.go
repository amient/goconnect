package avro

import (
	avrolib "github.com/amient/avro"
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)

type GenericProjector struct {
	TargetSchema avrolib.Schema
}

func (p *GenericProjector) InType() reflect.Type {
	return BinaryType
}

func (p *GenericProjector) OutType() reflect.Type {
	return GenericRecordType
}

func (p *GenericProjector) Materialize() func(input *goc.Element, context goc.PContext) {
	projections := make(map[fingerprint]avrolib.DatumReader)
	return func(input *goc.Element, context goc.PContext) {
		avroBinary := input.Value.(*Binary)
		var f fingerprint = avroBinary.Schema.Fingerprint()
		projection := projections[f]
		if projection == nil {
			projection = avrolib.NewDatumProjector(p.TargetSchema, avroBinary.Schema)
			projections[f] = projection
		}
		decodedRecord := avrolib.NewGenericRecord(p.TargetSchema)
		if err := projection.Read(decodedRecord, avrolib.NewBinaryDecoder(avroBinary.Data)); err != nil {
			panic(err)
		}
		context.Emit(&goc.Element{Value: decodedRecord})
	}
}

