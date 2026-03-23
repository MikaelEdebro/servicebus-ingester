package handler

import (
	"context"
	"log/slog"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

type UserUpdatedEventHandler struct {
	topic        string
	subscription string
	strategy     string
}

func NewUserUpdatedEventHandler() *UserUpdatedEventHandler {
	strategy := os.Getenv("SB_USER_UPDATED_STRATEGY")
	if strategy == "" {
		strategy = "single"
	}
	return &UserUpdatedEventHandler{
		topic:        os.Getenv("SB_USER_UPDATED_TOPIC"),
		subscription: os.Getenv("SB_USER_UPDATED_SUBSCRIPTION"),
		strategy:     strategy,
	}
}

func (h *UserUpdatedEventHandler) EventType() string    { return "UserUpdatedEvent" }
func (h *UserUpdatedEventHandler) Topic() string        { return h.topic }
func (h *UserUpdatedEventHandler) Subscription() string { return h.subscription }
func (h *UserUpdatedEventHandler) Strategy() string     { return h.strategy }

func (h *UserUpdatedEventHandler) HandleSingle(_ context.Context, _ string, event cloudevents.Event, _ string) error {
	slog.Info("User updated", "userId", event.ID())
	return nil
}

func (h *UserUpdatedEventHandler) HandleBatch(ctx context.Context, receiver *azservicebus.Receiver, items []ParsedMessage) error {
	for _, v := range items {
		slog.Info("User updated", "userId", v.Event.ID())
		if err := receiver.CompleteMessage(ctx, v.Msg, nil); err != nil {
			slog.Error("completing message", "error", err, "messageId", v.Msg.MessageID)
		}
	}
	return nil
}
