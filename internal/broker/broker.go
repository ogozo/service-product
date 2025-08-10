package broker

import (
	"fmt"
	"github.com/streadway/amqp"
)

type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewBroker(amqpURL string) (*Broker, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	return &Broker{conn: conn, channel: channel}, nil
}

func (b *Broker) Close() {
	b.channel.Close()
	b.conn.Close()
}