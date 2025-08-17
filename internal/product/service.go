package product

import (
	"context"

	pb "github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/ogozo/service-product/internal/broker"
	"github.com/ogozo/service-product/internal/logging"
	"go.uber.org/zap"
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

func (s *Service) HandleOrderCreatedEvent(ctx context.Context, event broker.OrderCreatedEvent) {
	logging.Info(ctx, "processing OrderCreated event", zap.String("order_id", event.OrderID))

	err := s.repo.UpdateStockInTx(ctx, event.Items)

	resultEvent := broker.StockUpdateResultEvent{
		OrderID: event.OrderID,
	}

	if err != nil {
		logging.Error(ctx, "stock update FAILED", err, zap.String("order_id", event.OrderID))
		resultEvent.Success = false
		resultEvent.Reason = err.Error()
	} else {
		logging.Info(ctx, "stock update SUCCESSFUL", zap.String("order_id", event.OrderID))
		resultEvent.Success = true
	}

	if err := s.broker.PublishStockUpdateResult(ctx, resultEvent); err != nil {
		logging.Error(ctx, "CRITICAL: failed to publish StockUpdateResult event", err, zap.String("order_id", event.OrderID))
	}
}
