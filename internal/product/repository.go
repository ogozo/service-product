package product

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
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
