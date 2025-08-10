package product

import (
	"context"

	pb "github.com/ogozo/proto-definitions/gen/go/product"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedProductServiceServer
	service *Service
}

func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

func (h *Handler) GetProduct(ctx context.Context, req *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	product, err := h.service.GetProduct(ctx, req.ProductId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "product not found: %v", err)
	}
	return &pb.GetProductResponse{Product: product}, nil
}
