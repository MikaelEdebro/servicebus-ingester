package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("servicebus-ingester-go/handler")

type EventHandler interface {
	EventType() string
	Topic() string
	Subscription() string
	Strategy() string
	HandleSingle(ctx context.Context, messageID string, event cloudevents.Event, rawBody string) error
	HandleBatch(ctx context.Context, receiver *azservicebus.Receiver, items []ParsedMessage) error
}

type ParsedMessage struct {
	Msg   *azservicebus.ReceivedMessage
	Event cloudevents.Event
}

type Dispatcher struct {
	handlers map[string]EventHandler
}

func NewDispatcher(handlers []EventHandler) *Dispatcher {
	m := make(map[string]EventHandler, len(handlers))
	for _, h := range handlers {
		m[h.EventType()] = h
	}
	return &Dispatcher{handlers: m}
}

func (d *Dispatcher) Handlers() []EventHandler {
	out := make([]EventHandler, 0, len(d.handlers))
	for _, h := range d.handlers {
		out = append(out, h)
	}
	return out
}

func (d *Dispatcher) DispatchSingle(ctx context.Context, receiver *azservicebus.Receiver, messages []*azservicebus.ReceivedMessage) {
	for _, msg := range messages {
		event, err := parseCloudEvent(msg)
		if err != nil {
			slog.Error("failed to parse message, letting lock expire", "messageId", msg.MessageID, "error", err)
			continue
		}

		slog.Info("received event", "type", event.Type(), "source", event.Source(), "id", event.ID(), "messageId", msg.MessageID, "strategy", "single")

		h, err := d.resolve(event.Type())
		if err != nil {
			slog.Error("no handler for event type, letting lock expire", "type", event.Type(), "messageId", msg.MessageID)
			continue
		}

		if err := h.HandleSingle(ctx, msg.MessageID, event, string(msg.Body)); err != nil {
			slog.Error("handling message, letting lock expire", "error", err, "messageId", msg.MessageID)
			continue
		}

		if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
			slog.Error("completing message", "error", err, "messageId", msg.MessageID)
		}
	}
}

func (d *Dispatcher) DispatchBatch(ctx context.Context, receiver *azservicebus.Receiver, messages []*azservicebus.ReceivedMessage) {
	groups := make(map[string][]ParsedMessage)

	for _, msg := range messages {
		event, err := parseCloudEvent(msg)
		if err != nil {
			slog.Error("failed to parse message, abandoning", "messageId", msg.MessageID, "error", err)
			if abErr := receiver.AbandonMessage(ctx, msg, nil); abErr != nil {
				slog.Warn("failed to abandon message, lock will expire", "messageId", msg.MessageID, "error", abErr)
			}
			continue
		}

		slog.Info("received event", "type", event.Type(), "source", event.Source(), "id", event.ID(), "messageId", msg.MessageID, "strategy", "batch")
		groups[event.Type()] = append(groups[event.Type()], ParsedMessage{Msg: msg, Event: event})
	}

	for eventType, items := range groups {
		h, err := d.resolve(eventType)
		if err != nil {
			slog.Error("no handler for event type, letting locks expire", "type", eventType, "count", len(items))
			continue
		}

		if err := h.HandleBatch(ctx, receiver, items); err != nil {
			slog.Error("handling batch, letting locks expire", "error", err, "type", eventType, "count", len(items))
		}
	}
}

func (d *Dispatcher) resolve(eventType string) (EventHandler, error) {
	h, ok := d.handlers[eventType]
	if !ok {
		return nil, fmt.Errorf("no handler registered for event type %q", eventType)
	}
	return h, nil
}

func parseCloudEvent(msg *azservicebus.ReceivedMessage) (cloudevents.Event, error) {
	var event cloudevents.Event
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		return event, fmt.Errorf("unmarshaling cloud event: %w", err)
	}
	return event, nil
}
