package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/ogozo/service-product/internal/broker"
	"github.com/ogozo/service-product/internal/config"
	"github.com/ogozo/service-product/internal/healthcheck"
	"github.com/ogozo/service-product/internal/logging"
	"github.com/ogozo/service-product/internal/observability"
	internalProduct "github.com/ogozo/service-product/internal/product"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func startMetricsServer(l *zap.Logger, port string) {
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		l.Info("metrics server started", zap.String("port", port))
		if err := http.ListenAndServe(port, mux); err != nil {
			l.Fatal("failed to start metrics server", zap.Error(err))
		}
	}()
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var cfg config.ProductConfig
	config.LoadConfig(&cfg)

	logging.Init(cfg.OtelServiceName)
	defer logging.Sync()

	logger := logging.FromContext(ctx)

	shutdown, err := observability.InitTracerProvider(ctx, cfg.OtelServiceName, cfg.OtelExporterEndpoint, logger)
	if err != nil {
		logger.Fatal("failed to initialize tracer provider", zap.Error(err))
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			logger.Fatal("failed to shutdown tracer provider", zap.Error(err))
		}
	}()

	startMetricsServer(logger, cfg.MetricsPort)

	var br *broker.Broker
	healthcheck.ConnectWithRetry(ctx, "RabbitMQ", 5, 2*time.Second, func() error {
		var err error
		br, err = broker.NewBroker(cfg.RabbitMQURL)
		return err
	})
	defer br.Close()
	if err := br.DeclareStockUpdateExchange(); err != nil {
		logger.Fatal("failed to declare exchange", zap.Error(err))
	}

	var rdb *redis.Client
	healthcheck.ConnectWithRetry(ctx, "Redis", 5, 2*time.Second, func() error {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
		return rdb.Ping(ctx).Err()
	})
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		logger.Fatal("failed to instrument redis tracing", zap.Error(err))
	}

	var dbpool *pgxpool.Pool
	healthcheck.ConnectWithRetry(ctx, "PostgreSQL", 5, 2*time.Second, func() error {
		var err error
		poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("failed to parse pgx config: %w", err)
		}
		poolConfig.ConnConfig.Tracer = otelpgx.NewTracer()
		dbpool, err = pgxpool.NewWithConfig(ctx, poolConfig)
		if err != nil {
			return err
		}
		return dbpool.Ping(ctx)
	})
	defer dbpool.Close()

	if err := otelpgx.RecordStats(dbpool, otelpgx.WithStatsMeterProvider(otel.GetMeterProvider())); err != nil {
		logger.Error("unable to record database stats", zap.Error(err))
	}

	productRepo := internalProduct.NewRepository(dbpool, rdb)
	productService := internalProduct.NewService(productRepo, br)
	productHandler := internalProduct.NewHandler(productService)

	if err := br.StartOrderCreatedConsumer(productService.HandleOrderCreatedEvent); err != nil {
		logger.Fatal("failed to start consumer", zap.Error(err))
	}

	lis, err := net.Listen("tcp", cfg.GRPCPort)
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err), zap.String("port", cfg.GRPCPort))
	}

	s := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	product.RegisterProductServiceServer(s, productHandler)

	logger.Info("gRPC server listening", zap.String("address", lis.Addr().String()))
	if err := s.Serve(lis); err != nil {
		logger.Fatal("failed to serve gRPC", zap.Error(err))
	}
}
