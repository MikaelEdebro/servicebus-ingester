package servicebus

import (
	"context"
	"encoding/json"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	"github.com/google/uuid"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type Sender struct {
	sender *azservicebus.Sender
}

func NewSender(client *azservicebus.Client, topicOrQueue string) (*Sender, error) {
	s, err := client.NewSender(topicOrQueue, nil)
	if err != nil {
		return nil, err
	}
	return &Sender{sender: s}, nil
}

func (s *Sender) SendCloudEvent(ctx context.Context, eventType, source string, data any) error {
	e := cloudevents.New()
	e.SetID(uuid.New().String())
	e.SetType(eventType)
	e.SetSource(source)
	if err := e.SetData(cloudevents.ApplicationJSON, data); err != nil {
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

	return s.sender.SendMessage(ctx, msg, nil)
}

func (s *Sender) Close(ctx context.Context) error {
	return s.sender.Close(ctx)
}
