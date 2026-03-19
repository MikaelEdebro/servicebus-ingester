package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/jackc/pgx/v5"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/MikaelEdebro/servicebus-ingester-go/internal/servicebus"
)

type parsedMessage struct {
	msg       *azservicebus.ReceivedMessage
	eventType string
	source    string
	rawBody   string
	data      any
}

func (h *Handler) HandleBatch(ctx context.Context, receiver *azservicebus.Receiver, messages []*azservicebus.ReceivedMessage) error {
	ctx, span := tracer.Start(ctx, "HandleBatch")
	span.SetAttributes(attribute.Int("messaging.batch_size", len(messages)))
	defer span.End()

	var valid []parsedMessage
	for _, msg := range messages {
		var event cloudevents.Event
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			slog.Error("failed to parse message, abandoning", "messageId", msg.MessageID, "error", err)
			if abErr := receiver.AbandonMessage(ctx, msg, nil); abErr != nil {
				slog.Warn("failed to abandon message, lock will expire", "messageId", msg.MessageID, "error", abErr)
			}
			continue
		}

		slog.Info("received event", "type", event.Type(), "source", event.Source(), "id", event.ID(), "messageId", msg.MessageID, "strategy", "batch")

		valid = append(valid, parsedMessage{
			msg:       msg,
			eventType: event.Type(),
			source:    event.Source(),
			rawBody:   string(msg.Body),
			data:      event.Data(),
		})
	}

	if len(valid) == 0 {
		return nil
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	_, dbSpan := tracer.Start(ctx, "db.InsertBatch")
	rows := make([][]any, len(valid))
	for i, v := range valid {
		rows[i] = []any{v.msg.MessageID, v.eventType, v.source, v.rawBody}
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
		events := make([]servicebus.OutboundEvent, len(valid))
		for i, v := range valid {
			events[i] = servicebus.OutboundEvent{
				EventType: v.eventType,
				Source:    v.source,
				Data:      v.data,
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

	for _, v := range valid {
		if err := receiver.CompleteMessage(ctx, v.msg, nil); err != nil {
			slog.Error("completing message", "error", err, "messageId", v.msg.MessageID)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "commit failed")
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}
