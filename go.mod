module github.com/amient/goconnect

go 1.12

require (
	github.com/amient/goconnect/io/kafka1 v0.0.0-20190819191431-05833c1f1719 // indirect
	gotest.tools/v3 v3.0.0
)

replace github.com/amient/goconnect/io/kafka1 => ./io/kafka1
