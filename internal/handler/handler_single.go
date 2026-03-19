package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/MikaelEdebro/servicebus-ingester-go/internal/db/queries"
)

func (h *Handler) HandleSingle(ctx context.Context, msg *azservicebus.ReceivedMessage) error {
	ctx, span := tracer.Start(ctx, "HandleMessage", trace.WithAttributes(
		attribute.String("messaging.message_id", msg.MessageID),
	))
	defer span.End()

	var event cloudevents.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "unmarshal failed")
		return fmt.Errorf("unmarshaling cloud event: %w", err)
	}

	span.SetAttributes(
		attribute.String("cloudevents.type", event.Type()),
		attribute.String("cloudevents.source", event.Source()),
		attribute.String("cloudevents.id", event.ID()),
	)

	slog.Info("received event", "type", event.Type(), "source", event.Source(), "id", event.ID(), "messageId", msg.MessageID, "strategy", "single")

	_, dbSpan := tracer.Start(ctx, "db.InsertMessage")
	err := h.queries.InsertMessage(ctx, queries.InsertMessageParams{
		MessageID: msg.MessageID,
		EventType: event.Type(),
		Source:    event.Source(),
		Body:      string(msg.Body),
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
