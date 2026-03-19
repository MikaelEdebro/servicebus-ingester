package servicebus

import (
	"context"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func (c *Consumer) processBatch(ctx context.Context, log *slog.Logger, receiver *azservicebus.Receiver, messages []*azservicebus.ReceivedMessage) {
	if err := c.batchHandler(ctx, receiver, messages); err != nil {
		log.Error("handling batch, letting locks expire", "error", err, "batchSize", len(messages))
	}
}
