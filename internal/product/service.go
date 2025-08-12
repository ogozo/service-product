package product

import (
	"context"
	"log"

	"github.com/ogozo/service-product/internal/broker"

	pb "github.com/ogozo/proto-definitions/gen/go/product"
)

type Service struct {
	repo   *Repository
	broker *broker.Broker
}

func NewService(repo *Repository, broker *broker.Broker) *Service {
	return &Service{repo: repo, broker: broker}
}

func (s *Service) CreateProduct(ctx context.Context, p *pb.Product) (*pb.Product, error) {
	return s.repo.CreateProduct(ctx, p)
}

func (s *Service) GetProduct(ctx context.Context, id string) (*pb.Product, error) {
	return s.repo.GetProductByID(ctx, id)
}

func (s *Service) HandleOrderCreatedEvent(event broker.OrderCreatedEvent) {
	log.Printf("Processing OrderCreated event for order %s", event.OrderID)

	err := s.repo.UpdateStockInTx(context.Background(), event.Items)

	resultEvent := broker.StockUpdateResultEvent{
		OrderID: event.OrderID,
	}

	if err != nil {
		log.Printf("❌ Stock update FAILED for order %s: %v", event.OrderID, err)
		resultEvent.Success = false
		resultEvent.Reason = err.Error()
	} else {
		log.Printf("✅ Stock update SUCCESSFUL for order %s", event.OrderID)
		resultEvent.Success = true
	}

	if err := s.broker.PublishStockUpdateResult(resultEvent); err != nil {
		log.Printf("CRITICAL: Failed to publish StockUpdateResult event for order %s: %v", event.OrderID, err)
	}
}
