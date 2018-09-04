package main

import (
	"github.com/amient/goconnect/pkg"
	"github.com/amient/goconnect/pkg/io/amqp091"
	"github.com/amient/goconnect/pkg/io/std"
	"github.com/amient/goconnect/pkg/coder/xmlcoder"
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

	decoder := new(xmlcoder.Decoder).Apply(source)

	sink := new(std.OutSink).Apply(decoder)

	//materialize and run the pipeline (this opens the connections to the respective backends)
	goconnect.Execute(source, sink, time.Second)

}
