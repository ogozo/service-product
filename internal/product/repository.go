package product

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pb_order "github.com/ogozo/proto-definitions/gen/go/order"
	pb "github.com/ogozo/proto-definitions/gen/go/product"
	"github.com/redis/go-redis/v9"
)

type Repository struct {
	db  *pgxpool.Pool
	rdb *redis.Client
}

func NewRepository(db *pgxpool.Pool, rdb *redis.Client) *Repository {
	return &Repository{db: db, rdb: rdb}
}

func (r *Repository) CreateProduct(ctx context.Context, p *pb.Product) (*pb.Product, error) {
	query := `INSERT INTO products (name, description, price, stock_quantity) VALUES ($1, $2, $3, $4) RETURNING id`
	err := r.db.QueryRow(ctx, query, p.Name, p.Description, p.Price, p.StockQuantity).Scan(&p.Id)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (r *Repository) GetProductByID(ctx context.Context, id string) (*pb.Product, error) {
	// 1. Önce Cache'i Kontrol Et
	productKey := fmt.Sprintf("product:%s", id)
	cachedProductJSON, err := r.rdb.Get(ctx, productKey).Result()
	if err == nil {
		// Cache'de bulundu (Cache Hit)
		var p pb.Product
		if err := json.Unmarshal([]byte(cachedProductJSON), &p); err == nil {
			return &p, nil
		}
	}

	// 2. Cache'de bulunamadı veya bir hata oluştu (Cache Miss), veritabanına git
	var p pb.Product
	query := `SELECT id, name, description, price, stock_quantity FROM products WHERE id = $1`
	err = r.db.QueryRow(ctx, query, id).Scan(&p.Id, &p.Name, &p.Description, &p.Price, &p.StockQuantity)
	if err != nil {
		return nil, err
	}

	// 3. Veritabanından gelen sonucu Cache'e yaz
	productJSON, err := json.Marshal(&p)
	if err == nil {
		// Cache'de 5 dakika boyunca sakla
		r.rdb.Set(ctx, productKey, productJSON, 5*time.Minute)
	}

	return &p, nil
}

func (r *Repository) UpdateStockInTx(ctx context.Context, items []*pb_order.OrderItem) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var productKeysToInvalidate []string
	for _, item := range items {
		var currentStock int32
		err := tx.QueryRow(ctx, "SELECT stock_quantity FROM products WHERE id = $1 FOR UPDATE", item.ProductId).Scan(&currentStock)
		if err != nil {
			return fmt.Errorf("failed to get stock for product %s: %w", item.ProductId, err)
		}

		if currentStock < item.Quantity {
			return fmt.Errorf("insufficient stock for product %s: available %d, requested %d", item.ProductId, currentStock, item.Quantity)
		}

		newStock := currentStock - item.Quantity
		_, err = tx.Exec(ctx, "UPDATE products SET stock_quantity = $1 WHERE id = $2", newStock, item.ProductId)
		if err != nil {
			return fmt.Errorf("failed to update stock for product %s: %w", item.ProductId, err)
		}
		productKeysToInvalidate = append(productKeysToInvalidate, fmt.Sprintf("product:%s", item.ProductId))
	}

	if len(productKeysToInvalidate) > 0 {
		r.rdb.Del(ctx, productKeysToInvalidate...)
	}

	return tx.Commit(ctx)
}
