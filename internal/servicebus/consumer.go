package servicebus

import (
	"context"
	"log/slog"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type MessageHandler func(ctx context.Context, msg *azservicebus.ReceivedMessage) error

type Consumer struct {
	client       *azservicebus.Client
	topic        string
	subscription string
	batchSize    int
	concurrency  int
	handler      MessageHandler
}

func NewConsumer(client *azservicebus.Client, topic, subscription string, batchSize, concurrency int, handler MessageHandler) *Consumer {
	return &Consumer{
		client:       client,
		topic:        topic,
		subscription: subscription,
		batchSize:    batchSize,
		concurrency:  concurrency,
		handler:      handler,
	}
}

// Run starts N concurrent receive-process loops. Each goroutine receives a batch,
// then processes messages sequentially — settlement must happen on the same receiver.
func (c *Consumer) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i := range c.concurrency {
		receiver, err := c.client.NewReceiverForSubscription(c.topic, c.subscription, nil)
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(id int, r *azservicebus.Receiver) {
			defer wg.Done()
			defer r.Close(context.Background())
			c.receiveLoop(ctx, id, r)
		}(i, receiver)
	}

	<-ctx.Done()
	slog.Info("shutting down consumer, waiting for in-flight messages")
	wg.Wait()
	slog.Info("consumer shutdown complete")
	return nil
}

func (c *Consumer) receiveLoop(ctx context.Context, id int, receiver *azservicebus.Receiver) {
	log := slog.With("consumer", id)

	for {
		messages, err := receiver.ReceiveMessages(ctx, c.batchSize, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("receiving messages", "error", err)
			continue
		}

		for _, msg := range messages {
			if err := c.handler(ctx, msg); err != nil {
				log.Error("handling message, letting lock expire", "error", err, "messageId", msg.MessageID)
				continue
			}

			if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
				log.Error("completing message", "error", err, "messageId", msg.MessageID)
			}
		}
	}
}
