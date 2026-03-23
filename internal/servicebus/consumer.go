package servicebus

import (
	"context"
	"log/slog"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type TopicConfig struct {
	Topic        string
	Subscription string
	Strategy     string
}

type DispatchFunc func(ctx context.Context, strategy string, receiver *azservicebus.Receiver, messages []*azservicebus.ReceivedMessage)

type Consumer struct {
	client    *azservicebus.Client
	batchSize int
	concurrency int
	topics    []TopicConfig
	dispatch  DispatchFunc
}

func NewConsumer(client *azservicebus.Client, batchSize, concurrency int, topics []TopicConfig, dispatch DispatchFunc) *Consumer {
	return &Consumer{
		client:      client,
		batchSize:   batchSize,
		concurrency: concurrency,
		topics:      topics,
		dispatch:    dispatch,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for _, tc := range c.topics {
		for i := range c.concurrency {
			receiver, err := c.client.NewReceiverForSubscription(tc.Topic, tc.Subscription, nil)
			if err != nil {
				return err
			}

			slog.Info("receiver started", "consumer", i, "topic", tc.Topic, "subscription", tc.Subscription, "strategy", tc.Strategy)

			wg.Add(1)
			go func(id int, r *azservicebus.Receiver, cfg TopicConfig) {
				defer wg.Done()
				defer r.Close(context.Background())
				c.receiveLoop(ctx, id, r, cfg)
			}(i, receiver, tc)
		}
	}

	<-ctx.Done()
	slog.Info("shutting down consumer, waiting for in-flight messages")
	wg.Wait()
	slog.Info("consumer shutdown complete")
	return nil
}

func (c *Consumer) receiveLoop(ctx context.Context, id int, receiver *azservicebus.Receiver, cfg TopicConfig) {
	log := slog.With("consumer", id, "topic", cfg.Topic, "subscription", cfg.Subscription)

	for {
		messages, err := receiver.ReceiveMessages(ctx, c.batchSize, nil)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("receiving messages", "error", err)
			continue
		}

		c.dispatch(ctx, cfg.Strategy, receiver, messages)
	}
}
