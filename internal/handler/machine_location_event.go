package handler

import (
	"context"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MikaelEdebro/servicebus-ingester-go/internal/db/queries"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/servicebus"
)

type MachineLocationEventHandler struct {
	pool    *pgxpool.Pool
	queries *queries.Queries
	sender  *servicebus.Sender

	topic        string
	subscription string
	strategy     string
}

func NewMachineLocationEventHandler(pool *pgxpool.Pool, q *queries.Queries, sender *servicebus.Sender) *MachineLocationEventHandler {
	strategy := os.Getenv("SB_MACHINE_LOCATION_STRATEGY")
	if strategy == "" {
		strategy = "single"
	}
	return &MachineLocationEventHandler{
		pool:         pool,
		queries:      q,
		sender:       sender,
		topic:        os.Getenv("SB_MACHINE_LOCATION_TOPIC"),
		subscription: os.Getenv("SB_MACHINE_LOCATION_SUBSCRIPTION"),
		strategy:     strategy,
	}
}

func (h *MachineLocationEventHandler) EventType() string    { return "MachineLocationEvent" }
func (h *MachineLocationEventHandler) Topic() string        { return h.topic }
func (h *MachineLocationEventHandler) Subscription() string { return h.subscription }
func (h *MachineLocationEventHandler) Strategy() string     { return h.strategy }

func (h *MachineLocationEventHandler) HandleSingle(ctx context.Context, messageID string, event cloudevents.Event, rawBody string) error {
	ctx, span := tracer.Start(ctx, "HandleMachineLocationEvent", trace.WithAttributes(
		attribute.String("messaging.message_id", messageID),
	))
	defer span.End()

	_, dbSpan := tracer.Start(ctx, "db.InsertMessage")
	err := h.queries.InsertMessage(ctx, queries.InsertMessageParams{
		MessageID: messageID,
		EventType: event.Type(),
		Source:    event.Source(),
		Body:      rawBody,
	})
	dbSpan.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "db insert failed")
		return fmt.Errorf("inserting message: %w", err)
	}

	if h.sender != nil {
		_, sendSpan := tracer.Start(ctx, "servicebus.SendCloudEvent")
		err := h.sender.SendCloudEvent(ctx, event.Type(), event.Source(), event.Data())
		sendSpan.End()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "send failed")
			return fmt.Errorf("sending response event: %w", err)
		}
	}

	return nil
}

func (h *MachineLocationEventHandler) HandleBatch(ctx context.Context, receiver *azservicebus.Receiver, items []ParsedMessage) error {
	ctx, span := tracer.Start(ctx, "HandleMachineLocationEventBatch")
	span.SetAttributes(attribute.Int("messaging.batch_size", len(items)))
	defer span.End()

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, dbSpan := tracer.Start(ctx, "db.InsertBatch")
	rows := make([][]any, len(items))
	for i, v := range items {
		rows[i] = []any{v.Msg.MessageID, v.Event.Type(), v.Event.Source(), string(v.Msg.Body)}
	}
	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"messages"},
		[]string{"message_id", "event_type", "source", "body"},
		pgx.CopyFromRows(rows),
	)
	dbSpan.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "batch insert failed")
		return fmt.Errorf("batch inserting messages: %w", err)
	}

	if h.sender != nil {
		_, sendSpan := tracer.Start(ctx, "servicebus.SendBatch")
		events := make([]servicebus.OutboundEvent, len(items))
		for i, v := range items {
			events[i] = servicebus.OutboundEvent{
				EventType: v.Event.Type(),
				Source:    v.Event.Source(),
				Data:      v.Event.Data(),
			}
		}
		err := h.sender.SendCloudEventBatch(ctx, events)
		sendSpan.End()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "batch send failed")
			return fmt.Errorf("batch sending events: %w", err)
		}
	}

	for _, v := range items {
		if err := receiver.CompleteMessage(ctx, v.Msg, nil); err != nil {
			return fmt.Errorf("completing message %s: %w", v.Msg.MessageID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "commit failed")
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}
