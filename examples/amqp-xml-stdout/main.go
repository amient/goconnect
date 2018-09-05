package main

import (
	"github.com/amient/goconnect/pkg"
	"github.com/amient/goconnect/pkg/coder/xmlc"
	"github.com/amient/goconnect/pkg/io/amqp091"
	"github.com/amient/goconnect/pkg/io/std"
	"time"
)

/**
	wrong:
		<?xml version =\"1.0\"?><xml attr=\"world\">hello</xml>
	correct:
		<?xml version ="1.0"?><xml attr="world">hello</xml>
*/

func main() {


	//declared pipeline stages (no i/o happens at this point, only channels are chained)
	source := &amqp091.Source{
		Uri:          "amqp://guest:guest@localhost:5672",
		Exchange:     "test-exchange",
		ExchangeType: "direct",
		QueueName:    "test",
		Group:        "simple-consumer",
		BindingKey:   "test-key",
	}

	decoder := new(xmlc.Decoder).Apply(source)

	encoder := new(xmlc.Encoder).Apply(decoder)

	sink := new(std.OutSink).Apply(encoder)

	//materialize and run the pipeline (this opens the connections to the respective backends)
	goconnect.Execute(source, sink, time.Second)

}
