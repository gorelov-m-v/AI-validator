package database

import (
	"context"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

type DB struct {
	conn   *sqlx.DB
	logger *zap.Logger
}

func New(dsn string, logger *zap.Logger) (*DB, error) {
	conn, err := sqlx.Connect("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	conn.SetMaxOpenConns(25)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info("database connection established", zap.String("driver", "pgx"))

	return &DB{
		conn:   conn,
		logger: logger,
	}, nil
}

func (db *DB) Close() error {
	db.logger.Info("closing database connection")
	return db.conn.Close()
}

func (db *DB) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return db.conn.PingContext(ctx)
}

func (db *DB) GetConn() *sqlx.DB {
	return db.conn
}

func (db *DB) BeginTx(ctx context.Context) (*sqlx.Tx, error) {
	return db.conn.BeginTxx(ctx, nil)
}
