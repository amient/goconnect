package main

import (
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/io/amqp091"
	"github.com/amient/goconnect/io/std"
	"github.com/amient/goconnect/io/xmlcoder"
	"time"
)

func main() {


	//wrong:
	// <?xml version =\"1.0\"?><xml attr=\"world\">hello</xml>
	//correct:
	// <?xml version ="1.0"?><xml attr="world">hello</xml>

	//declared pipeline stages (pure structs, no i/o happens at this point)

	source := &amqp091.Source {
		Uri:          "amqp://guest:guest@localhost:5672",
		Exchange:     "test-exchange",
		ExchangeType: "direct",
		QueueName:    "test",
		Group:        "simple-consumer",
		BindingKey:   "test-key",
	}

	decoder := new(xmlcoder.Decoder).Apply(source)

	sink := new(std.OutSink).Apply(decoder)

	//materialize and run the pipeline (this opens the connections to the respective backends)
	goconnect.Execute(source, sink, time.Second)




}