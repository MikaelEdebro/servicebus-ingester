package servicebus

import (
	"context"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func (c *Consumer) processSingle(ctx context.Context, log *slog.Logger, receiver *azservicebus.Receiver, messages []*azservicebus.ReceivedMessage) {
	for _, msg := range messages {
		if err := c.singleHandler(ctx, msg); err != nil {
			log.Error("handling message, letting lock expire", "error", err, "messageId", msg.MessageID)
			continue
		}

		if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
			log.Error("completing message", "error", err, "messageId", msg.MessageID)
		}
	}
}
