package handler

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/MikaelEdebro/servicebus-ingester-go/internal/db/queries"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/servicebus"
)

var tracer = otel.Tracer("servicebus-ingester-go/handler")

type Handler struct {
	pool    *pgxpool.Pool
	queries *queries.Queries
	sender  *servicebus.Sender
}

func New(pool *pgxpool.Pool, q *queries.Queries, sender *servicebus.Sender) *Handler {
	return &Handler{
		pool:    pool,
		queries: q,
		sender:  sender,
	}
}
