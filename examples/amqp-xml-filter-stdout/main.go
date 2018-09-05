package main

import (
	"github.com/amient/goconnect/pkg"
	"github.com/amient/goconnect/pkg/coder/xmlc"
	"github.com/amient/goconnect/pkg/io/amqp091"
	"github.com/amient/goconnect/pkg/io/std"
	"strings"
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

	filter := xmlc.Filter(func(in xmlc.Node) (bool, error) {
		//filter only valid xml that start with <?xml instruction
		return in.Children()[0].Type() == xmlc.ProcInst, nil
	}).Apply(decoder)

	modifiy := xmlc.Modify(func(in xmlc.Node) (xmlc.Node, error) {
		//if the xml is a single tag with text content, uppercase it
		if in.Children()[1].Children()[0].Type() == xmlc.Text {
			currentTest := in.Children()[1].Children()[0]
			if newText, err := xmlc.ReadNodeFromString(strings.ToUpper(currentTest.Text())); err != nil {
				return nil, err
			} else {
				in.Children()[1].Children()[0] = newText
			}
		}
		return in, nil
	}).Apply(filter)

	encoder := new(xmlc.Encoder).Apply(modifiy)

	sink := new(std.OutSink).Apply(encoder)

	//materialize and run the pipeline (this opens the connections to the respective backends)
	goconnect.Execute(source, sink, time.Second)

}
