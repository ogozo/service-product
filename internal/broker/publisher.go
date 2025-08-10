package broker

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func (b *Broker) DeclareStockUpdateExchange() error {
	return b.channel.ExchangeDeclare(
		"stock_update_exchange", // name
		"fanout",                // type
		true,                    // durable
		false,                   // auto-deleted
		false,                   // internal
		false,                   // no-wait
		nil,                     // arguments
	)
}

func (b *Broker) PublishStockUpdateResult(event StockUpdateResultEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	err = b.channel.Publish(
		"stock_update_exchange", // exchange
		"",                      // routing key
		false,                   // mandatory
		false,                   // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("✅ Published StockUpdateResult event for order %s (Success: %t)", event.OrderID, event.Success)
	return nil
}
