package main

import (
	"context"
	"log"
	"net"

	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/ogozo/service-product/internal/product"
	"google.golang.org/grpc"
)

const (
	dbURL    = "postgres://admin:secret@localhost:5432/ecommerce"
	grpcPort = ":50052"
)

func main() {
	dbpool, err := pgxpool.Connect(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()
	log.Println("Database connection successful for product service.")

	productRepo := product.NewRepository(dbpool)
	productService := product.NewService(productRepo)
	productHandler := product.NewHandler(productService)

	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterProductServiceServer(s, productHandler)

	log.Printf("Product gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
