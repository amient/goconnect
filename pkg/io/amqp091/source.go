package amqp091

import (
	"fmt"
	"github.com/amient/goconnect/pkg"
	"github.com/streadway/amqp" //AMQP 0.9.1 Compatible Client
	"log"
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
	output       chan *goconnect.Record
}

func (c *Source) Output() <-chan *goconnect.Record {
	if c.output == nil {
		c.output = make(chan *goconnect.Record)
	}
	return c.output
}

func (c *Source) Materialize() error {
	var err error

	log.Printf("dialing %q", c.Uri)
	c.conn, err = amqp.Dial(c.Uri)
	if err != nil {
		panic(err)
	}

	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		log.Printf("Closing AMQP source")
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		panic(err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", c.Exchange)
	if err = c.channel.ExchangeDeclare(
		c.Exchange,     // name of the Exchange
		c.ExchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", c.QueueName)
	queue, err := c.channel.QueueDeclare(
		c.QueueName, // name of the queue
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	)
	if err != nil {
		return fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)


	log.Printf("binding to Exchange (key %q)", c.BindingKey)
	if err := c.channel.QueueBind(c.QueueName, c.BindingKey, c.Exchange, false, nil, ); err != nil {
		panic(err)
	}
	log.Printf("Queue bound to Exchange, starting Consume (consumer Group %q)", c.Group)

	deliveries, err := c.channel.Consume(
		c.QueueName, c.Group, false, false, false, false, nil, )

	if err != nil {
		panic(err)
	}

	go func() {
		log.Printf("AMQP Source Started")
		defer log.Printf("AMQP Source Finished")
		defer close(c.output)
		for delivery := range deliveries {
			c.output <- &goconnect.Record{
				delivery.DeliveryTag,
				&c.empty, //TODO amqp protocol has a concept of RoutingKey (string) so this could be used
				&delivery.Body,
				&delivery.Timestamp,
			}
		}
	}()

	return nil
}

func (c *Source) Commit(position interface {}) {
	c.channel.Ack(position.(uint64), true)
}

func (c *Source) Close() error {

	if err := c.channel.Cancel(c.Group, true); err != nil {
		return fmt.Errorf("source cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil
}
