package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	pb_order "github.com/ogozo/proto-definitions/gen/go/order"
	"github.com/streadway/amqp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"
)

type TraceCarrier map[string]interface{}

func (c TraceCarrier) Get(key string) string {
	if val, ok := c[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (c TraceCarrier) Set(key, val string) {
	c[key] = val
}

func (c TraceCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tracer  trace.Tracer
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
	tracer := otel.Tracer("service-product.broker")
	return &Broker{conn: conn, channel: channel, tracer: tracer}, nil
}

func (b *Broker) Close() {
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}

type OrderCreatedEvent struct {
	OrderID    string                `json:"order_id"`
	UserID     string                `json:"user_id"`
	TotalPrice float64               `json:"total_price"`
	Items      []*pb_order.OrderItem `json:"items"`
}

func (b *Broker) StartOrderCreatedConsumer(handler func(ctx context.Context, event OrderCreatedEvent)) error {
	exchangeName := "orders_exchange"
	err := b.channel.ExchangeDeclare(exchangeName, "fanout", true, false, false, false, nil)
	if err != nil {
		return err
	}

	q, err := b.channel.QueueDeclare("product_service_order_created_queue", true, false, false, false, nil)
	if err != nil {
		return err
	}

	err = b.channel.QueueBind(q.Name, "", exchangeName, false, nil)
	if err != nil {
		return err
	}

	msgs, err := b.channel.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			carrier := make(TraceCarrier)
			for k, v := range d.Headers {
				carrier[k] = v
			}
			parentCtx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

			spanCtx, span := b.tracer.Start(parentCtx, exchangeName+" receive", trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemRabbitmq,
					semconv.MessagingDestinationName(exchangeName),
				),
			)

			log.Printf("📩 Received OrderCreated event: %s", d.Body)
			var event OrderCreatedEvent
			if err := json.Unmarshal(d.Body, &event); err != nil {
				log.Printf("Error unmarshalling event: %v", err)
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to unmarshal message")
				span.End()
				continue
			}

			handler(spanCtx, event)
			span.End()
		}
	}()
	log.Println("👂 Listening for OrderCreated events...")
	return nil
}

type StockUpdateResultEvent struct {
	OrderID string `json:"order_id"`
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

func (b *Broker) PublishStockUpdateResult(ctx context.Context, event StockUpdateResultEvent) error {
	exchangeName := "stock_update_exchange"
	spanCtx, span := b.tracer.Start(ctx, exchangeName+" publish", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemRabbitmq,
			semconv.MessagingDestinationName(exchangeName),
		),
	)
	defer span.End()

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	headers := make(TraceCarrier)
	otel.GetTextMapPropagator().Inject(spanCtx, headers)

	err = b.channel.Publish(exchangeName, "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
		Headers:     amqp.Table(headers),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish message")
		return fmt.Errorf("failed to publish message: %w", err)
	}

	log.Printf("✅ Published StockUpdateResult event for order %s (Success: %t)", event.OrderID, event.Success)
	return nil
}

func (b *Broker) DeclareStockUpdateExchange() error {
	return b.channel.ExchangeDeclare("stock_update_exchange", "fanout", true, false, false, false, nil)
}
