package main

import (
	"context"
	"log"
	"net"

	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/ogozo/service-product/internal/config"
	"github.com/ogozo/service-product/internal/product"
	"google.golang.org/grpc"
)

func main() {
	// 1. Yapılandırmayı yükle
	config.LoadConfig()
	cfg := config.AppConfig

	// 2. Veritabanı bağlantısını yapılandırmadan alarak kur
	dbpool, err := pgxpool.Connect(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()
	log.Println("Database connection successful for product service.")

	// 3. Bağımlılıkları enjekte et (Repository -> Service -> Handler)
	productRepo := product.NewRepository(dbpool)
	productService := product.NewService(productRepo)
	productHandler := product.NewHandler(productService)

	// 4. gRPC sunucusunu yapılandırmadan aldığı port ile başlat
	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", cfg.GRPCPort, err)
	}
	s := grpc.NewServer()
	pb.RegisterProductServiceServer(s, productHandler)

	log.Printf("Product gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
