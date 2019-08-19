package avro

import (
	avrolib "github.com/amient/avro"
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

func (p *GenericProjector) Materialize() func(input interface{}) interface{} {
	projections := make(map[avrolib.Fingerprint]avrolib.DatumReader)
	return func(input interface{}) interface{} {
		avroBinary := input.(*Binary)
		if f, err := avroBinary.Schema.Fingerprint(); err != nil {
			panic(err)
		} else {
			projection := projections[*f]
			if projection == nil {
				projection, err = avrolib.NewDatumProjector(p.TargetSchema, avroBinary.Schema)
				if err != nil {
					panic(err)
				}
				projections[*f] = projection
			}
			decodedRecord := avrolib.NewGenericRecord(p.TargetSchema)
			if err := projection.Read(decodedRecord, avrolib.NewBinaryDecoder(avroBinary.Data)); err != nil {
				panic(err)
			}
			return decodedRecord
		}
	}
}
