package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/ogozo/service-product/internal/broker"
	"github.com/ogozo/service-product/internal/config"
	"github.com/ogozo/service-product/internal/observability"
	"github.com/ogozo/service-product/internal/product"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	config.LoadConfig()
	cfg := config.AppConfig

	shutdown, err := observability.InitTracerProvider(ctx, cfg.OtelServiceName, cfg.OtelExporterEndpoint)
	if err != nil {
		log.Fatalf("failed to initialize tracer provider: %v", err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Fatalf("failed to shutdown tracer provider: %v", err)
		}
	}()

	br, err := broker.NewBroker(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}
	defer br.Close()
	if err := br.DeclareStockUpdateExchange(); err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}
	log.Println("RabbitMQ broker connected.")

	dbpool, err := pgxpool.Connect(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbpool.Close()
	log.Println("Database connection successful for product service.")

	productRepo := product.NewRepository(dbpool)
	productService := product.NewService(productRepo, br)
	productHandler := product.NewHandler(productService)

	if err := br.StartOrderCreatedConsumer(productService.HandleOrderCreatedEvent); err != nil {
		log.Fatalf("Failed to start consumer: %v", err)
	}

	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", cfg.GRPCPort, err)
	}
	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterProductServiceServer(s, productHandler)

	log.Printf("Product gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
