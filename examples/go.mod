module github.com/amient/goconnect/examples

go 1.13

require (
	github.com/amient/goconnect v0.0.0-20190819193555-d609bdff049f
	github.com/amient/goconnect/coder/avro v0.0.0-20190819193555-d609bdff049f
	github.com/amient/goconnect/io/amqp09 v0.0.0-20190819193555-d609bdff049f
	github.com/amient/goconnect/io/kafka1 v0.0.0-20190819193555-d609bdff049f
)

replace github.com/amient/goconnect => ./..

replace github.com/amient/goconnect/coder/avro => ./../coder/avro

replace github.com/amient/goconnect/io/amqp09 => ./../io/amqp09

replace github.com/amient/goconnect/io/kafka1 => ./../io/kafka1
