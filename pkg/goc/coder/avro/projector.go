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
	return AvroBinaryType
}

func (p *GenericProjector) OutType() reflect.Type {
	return GenericRecordType
}

func (p *GenericProjector) Materialize() func(input *goc.Element, context goc.PContext) {
	projections := make(map[uint32]avrolib.DatumReader)
	return func(input *goc.Element, context goc.PContext) {
		avroBinary := input.Value.(*AvroBinary)
		//FIXME instead of using schemaID implement Schema Fingerprints as per Avro Spec and then use the hash as the cache key
		projection := projections[avroBinary.SchemaID]
		if projection == nil {
			projection = avrolib.NewDatumProjector(p.TargetSchema, avroBinary.Schema)
			projections[avroBinary.SchemaID] = projection
		}
		decodedRecord := avrolib.NewGenericRecord(p.TargetSchema)
		if err := projection.Read(decodedRecord, avrolib.NewBinaryDecoder(avroBinary.Data)); err != nil {
			panic(err)
		}
		context.Emit(&goc.Element{Value: decodedRecord})
	}
}

//
//
//reader := avro.NewDatumReader(avroBinary.Schema)

//return decodedRecord
