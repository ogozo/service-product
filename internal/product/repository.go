package product

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	pb_order "github.com/ogozo/proto-definitions/gen/go/order"
	pb "github.com/ogozo/proto-definitions/gen/go/product"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
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
	var p pb.Product
	query := `SELECT id, name, description, price, stock_quantity FROM products WHERE id = $1`
	err := r.db.QueryRow(ctx, query, id).Scan(&p.Id, &p.Name, &p.Description, &p.Price, &p.StockQuantity)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *Repository) UpdateStockInTx(ctx context.Context, items []*pb_order.OrderItem) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

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
	}

	return tx.Commit(ctx)
}
