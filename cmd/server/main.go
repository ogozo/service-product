package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/ogozo/service-product/internal/broker"
	"github.com/ogozo/service-product/internal/config"
	"github.com/ogozo/service-product/internal/observability"
	"github.com/ogozo/service-product/internal/product"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
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

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisURL,
	})
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		log.Fatalf("failed to instrument redis tracing: %v", err)
	}
	if err := redisotel.InstrumentMetrics(rdb); err != nil {
		log.Fatalf("failed to instrument redis metrics: %v", err)
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Unable to connect to Redis: %v", err)
	}
	log.Println("Redis connection successful, with OTel instrumentation.")

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to parse pgx config: %v", err)
	}

	poolConfig.ConnConfig.Tracer = otelpgx.NewTracer()

	dbpool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbpool.Close()

	if err := otelpgx.RecordStats(dbpool, otelpgx.WithStatsMeterProvider(otel.GetMeterProvider())); err != nil {
		log.Printf("WARN: unable to record database stats: %v", err)
	}

	log.Println("Database connection successful for product service, with OTel instrumentation.")

	productRepo := product.NewRepository(dbpool, rdb)
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
