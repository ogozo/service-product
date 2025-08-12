package broker

import pb_order "github.com/ogozo/proto-definitions/gen/go/order"

type OrderCreatedEvent struct {
	OrderID    string                `json:"order_id"`
	UserID     string                `json:"user_id"`
	TotalPrice float64               `json:"total_price"`
	Items      []*pb_order.OrderItem `json:"items"`
}

type StockUpdateResultEvent struct {
	OrderID string `json:"order_id"`
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}
