package product

import (
	"context"

	pb "github.com/ogozo/proto-definitions/gen/go/product"
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) CreateProduct(ctx context.Context, p *pb.Product) (*pb.Product, error) {
	return s.repo.CreateProduct(ctx, p)
}

func (s *Service) GetProduct(ctx context.Context, id string) (*pb.Product, error) {
	return s.repo.GetProductByID(ctx, id)
}
