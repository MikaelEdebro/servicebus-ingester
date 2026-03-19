package servicebus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"
)

type OutboundEvent struct {
	EventType string
	Source    string
	Data      any
}

func (s *Sender) SendCloudEventBatch(ctx context.Context, events []OutboundEvent) error {
	batch, err := s.sender.NewMessageBatch(ctx, nil)
	if err != nil {
		return fmt.Errorf("creating message batch: %w", err)
	}

	for _, evt := range events {
		e := cloudevents.New()
		e.SetID(uuid.New().String())
		e.SetType(evt.EventType)
		e.SetSource(evt.Source)
		if err := e.SetData(cloudevents.ApplicationJSON, evt.Data); err != nil {
			return fmt.Errorf("setting cloud event data: %w", err)
		}

		body, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshaling cloud event: %w", err)
		}

		contentType := "application/cloudevents+json"
		msg := &azservicebus.Message{
			Body:        body,
			ContentType: &contentType,
		}

		if err := batch.AddMessage(msg, nil); err != nil {
			if err := s.sender.SendMessageBatch(ctx, batch, nil); err != nil {
				return fmt.Errorf("sending message batch: %w", err)
			}

			batch, err = s.sender.NewMessageBatch(ctx, nil)
			if err != nil {
				return fmt.Errorf("creating message batch: %w", err)
			}

			if err := batch.AddMessage(msg, nil); err != nil {
				return fmt.Errorf("single message too large for batch: %w", err)
			}
		}
	}

	if batch.NumMessages() > 0 {
		if err := s.sender.SendMessageBatch(ctx, batch, nil); err != nil {
			return fmt.Errorf("sending message batch: %w", err)
		}
	}

	return nil
}
