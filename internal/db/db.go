package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MikaelEdebro/servicebus-ingester/internal/config"
)

func NewPool(ctx context.Context, cfg *config.Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL())
	if err != nil {
		return nil, err
	}

	poolCfg.MaxConns = int32(cfg.DBMaxConns)
	poolCfg.MaxConnIdleTime = time.Duration(cfg.DBConnIdleTimeMinutes) * time.Minute
	poolCfg.MaxConnLifetime = time.Duration(cfg.DBConnLifeTimeMinutes) * time.Minute

	if cfg.DBSchema != "" {
		poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, err := conn.Exec(ctx, fmt.Sprintf("SET search_path TO %s, public", cfg.DBSchema))
			return err
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}
