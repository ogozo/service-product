package broker

import (
	"encoding/json"
	"log"
)

func (b *Broker) StartOrderCreatedConsumer(handler func(event OrderCreatedEvent)) error {
	err := b.channel.ExchangeDeclare("orders_exchange", "fanout", true, false, false, false, nil)
	if err != nil {
		return err
	}

	q, err := b.channel.QueueDeclare(
		"product_service_order_created_queue", // name
		true,                                  // durable
		false,                                 // delete when unused
		false,                                 // exclusive
		false,                                 // no-wait
		nil,                                   // arguments
	)
	if err != nil {
		return err
	}

	err = b.channel.QueueBind(q.Name, "", "orders_exchange", false, nil)
	if err != nil {
		return err
	}

	msgs, err := b.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			log.Printf("📩 Received OrderCreated event: %s", d.Body)
			var event OrderCreatedEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Error unmarshalling event: %v", err)
				continue
			}
			handler(event)
		}
	}()
	log.Println("👂 Listening for OrderCreated events...")
	return nil
}
