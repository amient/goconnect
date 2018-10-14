package avro

import (
	"bytes"
	"fmt"
	avrov0 "github.com/amient/avro"
	"github.com/amient/goconnect/pkg/goc/coder/avro/resolver.fixtures"
	"log"
	"os"
	"reflect"
	"testing"
)

//https://godoc.org/gopkg.in/avro.v0

//TODO test projection
//TODO add maps
//TODO consider enums
//TODO make PR for aliases in goavro (https://github.com/linkedin/goavro) using struct field Tags
//TODO consider prefixes and FIXED avro type
//TODO make avro schema reflector for structs

var (
	schemaV1 avrov0.Schema
	schemaV2 avrov0.Schema
	schemaV3 avrov0.Schema
)
func init() {
	// Parse the schemaV2 first
	var err error
	if schemaV1, err = avrov0.ParseSchema(resolver_fixtures.UserSchemaV1); err != nil {
		panic(err)
	}
	if schemaV2, err = avrov0.ParseSchema(resolver_fixtures.UserSchemaV2); err != nil {
		panic(err)
	}

}

func TestForwardProjection(t *testing.T) {
	//codecV1, err := goavro.NewCodec(resolver_fixtures.UserSchemaV1)
	//if err != nil {
	//	panic(err)
	//}


	////Convert V1 Binary From Native
	//binary, err := codecV1.BinaryFromNative(nil, map[string]interface{}{
	//	"unknown": "xyz",
	//	"first_name": "John",
	//	"last_name":  "Snow",
	//})
	//if err != nil {
	//	panic(err)
	//} else {
	//	//fmt.Printf("%v\n", binary)
	//}


	//genericV1, _, err := codecV1.NativeFromBinary(binary)
	//if err != nil {
	//	panic(err)
	//}

	//genericV2, _, err := codecV2.NativeFromBinaryProject(codecV1, binary)
	//if err != nil {
	//	panic(err)
	//}
	//
	//
	////Decode with V2 Codec
	//codecV2, err := goavro.NewCodec(resolver_fixtures.UserSchemaV2)
	//if err != nil {
	//	panic(err)
	//}
	//
	//fmt.Printf("!!! %v\n", genericV1)
	//
	//genericV2 := codecV2.projectGeneric(genericV1)// GenericProjected(*codecV2, genericV1)
	//
	//userOut := GenericToNative(reflect.TypeOf(resolver_fixtures.UserV2{}), genericV2).(resolver_fixtures.UserV2)
	////
	//if userOut.FirstName != "John" || userOut.LastName != "Snow" || len(userOut.Errors) != 0 {
	//	panic(fmt.Errorf("%v\n", userOut))
	//}
}


func TestPromotionAndProjection(t *testing.T) {

	recordV1 := &resolver_fixtures.UserV1{
		First_name: []byte("John"),
		Last_name:  []byte("Snow"),
		Phone: nil,
		Errors:    []string{},
		Address: &resolver_fixtures.Address{
			Address1: "1106 Pennsylvania Avenue",
			City:     "Wilmington",
			State:    "DE",
			Zip:      19806,
		},
	}

	writer := avrov0.NewSpecificDatumWriter().SetSchema(schemaV1)
	buffer := new(bytes.Buffer)
	encoder := avrov0.NewBinaryEncoder(buffer)
	if err := writer.Write(recordV1, encoder); err != nil {
		panic(err)
	}
	binary := buffer.Bytes()
	log.Println(binary)


	reader := avrov0.NewSpecificDatumReader().SetSchema(schemaV1)
	decoder := avrov0.NewBinaryDecoder(binary)
	decodedRecord := new(resolver_fixtures.UserV1)
	if err := reader.Read(decodedRecord, decoder); err != nil {
		panic(err)
	}
	log.Println(decodedRecord)



	//generic1 := map[string]interface{}{
	//	"FirstName": []byte("John"),
	//	"LastName":  []byte("Snow"),
	//}
	//
	////Convert Binary From Native
	//binary, err := codec.BinaryFromNative(nil, generic1)
	//if err != nil {
	//	panic(err)
	//}
	//
	////fmt.Printf("%v\n", binary)
	//
	//generic, _, err := codec.NativeFromBinary(binary)
	//if err != nil {
	//	panic(err)
	//}
	//userOut := GenericToNative(reflect.TypeOf(resolver_fixtures.UserV2{}), generic).(resolver_fixtures.UserV2)
	//
	////fmt.Printf("%v\n", userOut)
	//if userOut.FirstName != "John" || userOut.LastName != "Snow" || len(userOut.Errors) != 0 {
	//	panic(fmt.Errorf("%v\n", userOut))
	//}
}

func TestBinaryConversion(t *testing.T) {

	//Sample Record
	var phone int32 = 44234234
	recordV2 := &resolver_fixtures.UserV2{
		FirstName: "John",
		LastName:  "Snow",
		Phone: &phone,
		Errors:    []string{"error1", "error2"},
		Address: &resolver_fixtures.Address{
			Address1: "1106 Pennsylvania Avenue",
			City:     "Wilmington",
			State:    "DE",
			Zip:      19806,
		},
	}

	writer := avrov0.NewSpecificDatumWriter().SetSchema(schemaV2)
	buffer := new(bytes.Buffer)
	encoder := avrov0.NewBinaryEncoder(buffer)
	if err := writer.Write(recordV2, encoder); err != nil {
		panic(err)
	}
	binary := buffer.Bytes()
	log.Println(binary)

	reader := avrov0.NewSpecificDatumReader().SetSchema(schemaV2)
	decoder := avrov0.NewBinaryDecoder(binary)
	decodedRecordV2 := new(resolver_fixtures.UserV2)
	if err := reader.Read(decodedRecordV2, decoder); err != nil {
		panic(err)
	}
	log.Println(decodedRecordV2)

	if ok := reflect.DeepEqual(recordV2, decodedRecordV2); !ok {
		fmt.Fprintf(os.Stderr, "struct Compare Failed ok=%t\n", ok)
		os.Exit(1)
	}

}
