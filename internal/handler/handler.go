package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"

	"github.com/MikaelEdebro/servicebus-ingester/internal/db/queries"
	"github.com/MikaelEdebro/servicebus-ingester/internal/servicebus"
)

type Handler struct {
	queries *queries.Queries
	sender  *servicebus.Sender
}

func New(q *queries.Queries, sender *servicebus.Sender) *Handler {
	return &Handler{
		queries: q,
		sender:  sender,
	}
}

func (h *Handler) Handle(ctx context.Context, msg *azservicebus.ReceivedMessage) error {
	var event cloudevents.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return fmt.Errorf("unmarshaling cloud event: %w", err)
	}

	slog.Info("received event", "type", event.Type(), "source", event.Source(), "id", event.ID(), "messageId", msg.MessageID)

	if err := h.queries.InsertMessage(ctx, queries.InsertMessageParams{
		MessageID: msg.MessageID,
		EventType: event.Type(),
		Source:    event.Source(),
		Body:     msg.Body,
	}); err != nil {
		return fmt.Errorf("inserting message: %w", err)
	}

	return nil
}
