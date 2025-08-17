package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/ogozo/service-product/internal/broker"
	"github.com/ogozo/service-product/internal/config"
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

	br, err := broker.NewBroker(cfg.RabbitMQURL)
	if err != nil {
		logger.Fatal("failed to create broker", zap.Error(err))
	}
	defer br.Close()
	if err := br.DeclareStockUpdateExchange(); err != nil {
		logger.Fatal("failed to declare exchange", zap.Error(err))
	}
	logger.Info("RabbitMQ broker connected")

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisURL})
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		logger.Fatal("failed to instrument redis tracing", zap.Error(err))
	}
	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatal("unable to connect to Redis", zap.Error(err))
	}
	logger.Info("Redis connection successful, with OTel instrumentation")

	poolConfig, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		logger.Fatal("failed to parse pgx config", zap.Error(err))
	}
	poolConfig.ConnConfig.Tracer = otelpgx.NewTracer()

	dbpool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		logger.Fatal("unable to connect to database", zap.Error(err))
	}
	defer dbpool.Close()

	if err := otelpgx.RecordStats(dbpool, otelpgx.WithStatsMeterProvider(otel.GetMeterProvider())); err != nil {
		logger.Error("unable to record database stats", zap.Error(err))
	}
	logger.Info("database connection successful, with OTel instrumentation")

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
