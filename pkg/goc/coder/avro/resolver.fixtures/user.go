package resolver_fixtures

var (
	UserSchemaV1 = `{
    "type":	"record",
    "name": "UserV2",
	"namespace": "resolver.fixtures",
    "fields": [
        { "name": "Errors", "type": { "type": "array", "items":"string" }, "default": []},
        { "name": "Phone", "type": ["null", "int"], "default": null },
        { "name": "first_name", "type": "bytes"},
        { "name": "last_name", "type": "bytes"},
        { "name": "Address", "type": ["null",{
            "type":	"record",
            "name": "Address",
			"namespace": "resolver.fixtures",
            "fields": [
                { "name": "Address1", "type": "string" },
                { "name": "Address2", "type": ["null", "string"], "default": null },
                { "name": "City", "type": "string" },
                { "name": "State", "type": "string" },
                { "name": "Zip", "type": "int" }
            ]
        }],"default":null}
    ]
}`
	UserSchemaV2 = `{
    "type":	"record",
    "name": "UserV2",
	"namespace": "resolver.fixtures",
    "fields": [
        { "name": "FirstName", "type": "string", "aliases": ["first_name"]},
        { "name": "LastName", "type": "string", "aliases": ["last_name"]}, 
		{ "name": "Phone", "type": ["null", "int"], "default": null },
		{ "name": "Errors", "type": { "type": "array", "items":"string" }, "default": []},
 		{ "name": "Address", "type": ["null",{
            "type":	"record",
            "name": "Address",
			"namespace": "resolver.fixtures",
            "fields": [
                { "name": "Address1", "type": "string" },
                { "name": "Address2", "type": ["null", "string"], "default": null },
                { "name": "City", "type": "string" },
                { "name": "State", "type": "string" },
                { "name": "Zip", "type": "int" }
            ]
        }],"default":null}
    ]
}`
)

type UserV1 struct {
	First_name []byte `avro:first_name`
	Last_name  []byte `avro:last_name`
	Address    *Address
	Errors     []string
	Phone      *int32
}

type UserV2 struct {
	FirstName string
	LastName  string
	Address   *Address
	Errors    []string
	Phone     *int32
}

type Address struct {
	Address1 string
	Address2 *string
	City     string
	State    string
	Zip      int32
}
