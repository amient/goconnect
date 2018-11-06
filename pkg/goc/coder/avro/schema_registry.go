package avro

import (
	"encoding/binary"
	"github.com/amient/goconnect/pkg/goc"
	"reflect"
)


//TODO SchemaRegistryEncoder AvroBinary > goc.Binary ; that detects generic or specific records on input, compares and optionally registers schemas

type SchemaRegistry struct {
	Url string
	//TODO add ca cert
	//TODO add ssl cert
	//TODO add ssl key
	//TODO add ssl key password
}

func (cf *SchemaRegistry) InType() reflect.Type {
	return goc.BinaryType
}

func (cf *SchemaRegistry) OutType() reflect.Type {
	return AvroBinaryType
}

func (cf *SchemaRegistry) Materialize() func(input *goc.Element, context goc.PContext) {
	client := &schemaRegistryClient{url: cf.Url}
	return func(input *goc.Element, context goc.PContext) {
		bytes := input.Value.([]byte)
		switch bytes[0] {
		case 0:
			schemaId := binary.BigEndian.Uint32(bytes[1:])
			schema := client.get(schemaId)
			context.Emit(&goc.Element{Value: &AvroBinary{
				Schema:   schema,
				SchemaID: schemaId,
				Data:     bytes[5:],
			}})
		case 1:
			panic("avro binary header incorrect")
		}
	}
}
