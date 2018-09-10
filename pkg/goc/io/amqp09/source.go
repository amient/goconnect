package amqp09

import (
	"fmt"
	"github.com/amient/goconnect/pkg/goc"
	"github.com/streadway/amqp"
	"log"
	"reflect"
)

type Source struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	Uri          string
	Exchange     string
	ExchangeType string
	QueueName    string
	Group        string
	BindingKey   string
	empty        []byte
}

func (source *Source) OutType() reflect.Type {
	return goc.ByteArrayType
}

func (source *Source) Run(output chan *goc.Element) {
	var err error

	log.Printf("dialing %q", source.Uri)
	source.conn, err = amqp.Dial(source.Uri)
	if err != nil {
		panic(err)
	}

	go func() {
		<-source.conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("Closing AMQP source")
	}()

	log.Printf("got Connection, getting channel")
	source.channel, err = source.conn.Channel()
	if err != nil {
		panic(err)
	}

	log.Printf("got channel, declaring Exchange (%q)", source.Exchange)
	if err = source.channel.ExchangeDeclare(
		source.Exchange,     // name of the Exchange
		source.ExchangeType, // type
		true,                // durable
		false,               // delete when complete
		false,               // internal
		false,               // noWait
		nil,                 // arguments
	); err != nil {
		panic(fmt.Errorf("exchange Declare: %source", err))
	}

	log.Printf("declared Exchange, declaring Queue %q", source.QueueName)
	queue, err := source.channel.QueueDeclare(
		source.QueueName, // name of the queue
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %source", err))
	}

	log.Printf("declared Queue (%q %d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	log.Printf("binding to Exchange (key %q)", source.BindingKey)
	if err := source.channel.QueueBind(source.QueueName, source.BindingKey, source.Exchange, false, nil, ); err != nil {
		panic(err)
	}
	log.Printf("Queue bound to Exchange, starting Consume (consumer Group %q)", source.Group)

	deliveries, err := source.channel.Consume(
		source.QueueName, source.Group, false, false, false, false, nil, )

	if err != nil {
		panic(err)
	}

	for delivery := range deliveries {
		output <- &goc.Element{
			Timestamp:  &delivery.Timestamp,
			Checkpoint: goc.Checkpoint{
				0: delivery.DeliveryTag,
			},
			Value:      delivery.Body,
		}
	}
}

func (source *Source) Commit(checkpoint goc.Checkpoint) error {
	return source.channel.Ack(checkpoint[0].(uint64), true)
}

func (source *Source) Close() error {

	if err := source.channel.Cancel(source.Group, true); err != nil {
		return fmt.Errorf("source cancel failed: %source", err)
	}

	if err := source.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %source", err)
	}

	return nil
}
