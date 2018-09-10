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

func (it *Source) OutType() reflect.Type {
	return reflect.TypeOf([]byte(nil))
}

func (s *Source) Run(output chan *goc.Element) {
	var err error

	log.Printf("dialing %q", s.Uri)
	s.conn, err = amqp.Dial(s.Uri)
	if err != nil {
		panic(err)
	}

	go func() {
		<-s.conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("Closing AMQP source")
	}()

	log.Printf("got Connection, getting channel")
	s.channel, err = s.conn.Channel()
	if err != nil {
		panic(err)
	}

	log.Printf("got channel, declaring Exchange (%q)", s.Exchange)
	if err = s.channel.ExchangeDeclare(
		s.Exchange,     // name of the Exchange
		s.ExchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		panic(fmt.Errorf("exchange Declare: %s", err))
	}

	log.Printf("declared Exchange, declaring Queue %q", s.QueueName)
	queue, err := s.channel.QueueDeclare(
		s.QueueName, // name of the queue
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	)
	if err != nil {
		panic(fmt.Errorf("Queue Declare: %s", err))
	}

	log.Printf("declared Queue (%q %d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	log.Printf("binding to Exchange (key %q)", s.BindingKey)
	if err := s.channel.QueueBind(s.QueueName, s.BindingKey, s.Exchange, false, nil, ); err != nil {
		panic(err)
	}
	log.Printf("Queue bound to Exchange, starting Consume (consumer Group %q)", s.Group)

	deliveries, err := s.channel.Consume(
		s.QueueName, s.Group, false, false, false, false, nil, )

	if err != nil {
		panic(err)
	}

	log.Printf("AMQP RootFn Started")
	defer log.Printf("AMQP RootFn Finished")

	for delivery := range deliveries {
		output <- &goc.Element{
			Timestamp:  &delivery.Timestamp,
			Checkpoint: delivery.DeliveryTag,
			Value:      delivery.Body,
		}
	}
}

func (s *Source) Commit(checkpoint goc.Checkpoint) error {
	return s.channel.Ack(checkpoint.(uint64), true)
}

func (s *Source) Close() error {

	if err := s.channel.Cancel(s.Group, true); err != nil {
		return fmt.Errorf("source cancel failed: %s", err)
	}

	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil
}
