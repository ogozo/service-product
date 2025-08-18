# Product Service

[![Go Version](https://img.shields.io/badge/Go-1.22%2B-blue.svg)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A microservice that manages the product catalog, inventory, and pricing for the E-Commerce Microservice Platform.

## 📖 Overview

This service is the source of truth for all product-related information. Its key responsibilities are:

*   **Product Catalog Management:** Provides functionality to create, read, and update product details.
*   **Inventory Control:** Manages the stock quantity for each product. It plays a crucial role in the order fulfillment saga by updating stock levels.
*   **Performance Caching:** Utilizes a Redis cache to accelerate read operations for frequently accessed product data, reducing the load on the primary PostgreSQL database.

The service exposes its synchronous operations via a gRPC API and participates in the asynchronous, event-driven saga via RabbitMQ.

## 🚀 Architecture

The Product Service follows a Clean Architecture pattern (Handler, Service, Repository). A key feature of its architecture is the caching layer implemented within the repository.

```mermaid
graph TD
    subgraph "Request Flow"
        GW[🌐 API Gateway] -- gRPC --> Handler;
        Handler -- Calls --> BusinessService[Service];
        BusinessService -- Uses --> Repository;
    end
    
    subgraph "Data & Cache Flow"
        Repository -- "Cache Check" --> RD[(⚡ Redis)];
        Repository -- "DB Fallback (Cache Miss)" --> PG[(🗄️ PostgreSQL)];
        Repository -- "Cache Write" --> RD;
    end

    subgraph "Event Flow (Saga)"
        RMQin[("📨 RabbitMQ")] -- "OrderCreated Event" --> Consumer;
        Consumer -- Calls --> BusinessService;
        BusinessService -- "Update Stock" --> PG;
        BusinessService -- Publishes --> RMQout[("📨 RabbitMQ")];
    end

    subgraph "Observability"
        Handler -- "Traces/Logs/Metrics" --> OBS[(🔭 Observability Stack)];
        Repository -- "Traces/Logs" --> OBS;
        Consumer -- "Traces/Logs" --> OBS;
    end
```

### Caching Strategy

When a request is made to fetch a product (`GetProductByID`), the repository first checks Redis for the data.
*   **Cache Hit:** If the product data is found in Redis, it is returned directly, avoiding a database query.
*   **Cache Miss:** If the data is not in Redis, the repository queries the PostgreSQL database, returns the data to the service, and asynchronously writes the result to Redis with a Time-To-Live (TTL) for future requests.
*   **Cache Invalidation:** When stock is updated as part of an order saga, the corresponding product entry in the Redis cache is deleted (`invalidated`) to ensure that subsequent reads fetch the most up-to-date stock information from the database.

## 🛠️ Tech Stack

| Category | Technology |
| :--- | :--- |
| **Language** | Go |
| **Communication** | gRPC (Server), RabbitMQ (Consumer/Producer), Protobuf |
| **Database** | PostgreSQL (`pgx/v5` driver) |
| **Cache** | Redis (`go-redis/v9`) |
| **Observability** | OpenTelemetry (`otelgrpc`, `otelpgx`, `redisotel`), Prometheus, Zap |
| **Containerization** | Docker |

## ⚙️ Setup & Running

This service is designed to be run as part of the complete platform using Docker Compose.

### Prerequisites

*   Docker & Docker Compose
*   All project repositories must be cloned at the same directory level.

### Running

The service is automatically built and started when you run the main `docker-compose up` command from the `docker-compose-environment` repository.

For detailed instructions, please see the [main project README](https://github.com/ogozo/docker-compose-environment/blob/main/README.md).

### Environment Variables

The service is configured via the `env/product.env` file in the main compose directory.

| Variable | Description | Example |
| :--- | :--- | :--- |
| `GRPC_PORT` | The port on which the gRPC server listens. | `:50052` |
| `METRICS_PORT`| The port for the `/metrics` HTTP endpoint. | `:9002` |
| `DATABASE_URL`| Connection string for the PostgreSQL database. | `postgres://admin:secret@postgres-db:5432/ecommerce`|
| `REDIS_URL` | Address of the Redis cache server. | `redis-cache:6379` |
| `RABBITMQ_URL`| Connection string for the RabbitMQ broker. | `amqp://guest:guest@rabbitmq:5672/` |
| `OTEL_SERVICE_NAME`| Service name for observability. | `service-product` |
| `OTEL_EXPORTER_OTLP_ENDPOINT`| OTLP endpoint for Jaeger. | `jaeger-tracer:4317` |

## 📡 gRPC & Event API

This service exposes its functionality via the `ProductService` gRPC interface and interacts with RabbitMQ events.

### gRPC API

*   **`rpc GetProduct(GetProductRequest) returns (GetProductResponse)`**: Retrieves details for a single product, utilizing the Redis cache.
*   **`rpc CreateProduct(CreateProductRequest) returns (CreateProductResponse)`**: Creates a new product in the database. (Admin only)
*   **`rpc UpdateStock(UpdateStockRequest) returns (UpdateStockResponse)`**: (Internal) A method to update stock levels, typically called by the saga.

### Asynchronous API (Events)

*   **Listens for:** `OrderCreated` Event
    *   **Action:** When this event is received, the service attempts to decrease the stock for the products in the order.
*   **Publishes:** `StockUpdateResult` Event
    *   **Action:** After processing the stock update, it publishes an event indicating whether the update was successful or failed (e.g., due to insufficient stock).
