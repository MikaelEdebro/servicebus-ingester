package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MikaelEdebro/servicebus-ingester-go/internal/config"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/db"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/db/queries"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/handler"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/health"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/servicebus"
	"github.com/MikaelEdebro/servicebus-ingester-go/internal/tracing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("loading config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	tp, err := tracing.Init(ctx, "servicebus-ingester-go")
	if err != nil {
		slog.Error("initializing tracing", "error", err)
		os.Exit(1)
	}
	defer tp.Shutdown(context.Background())

	pool, err := db.NewPool(ctx, cfg)
	if err != nil {
		slog.Error("connecting to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	sbClient, err := servicebus.NewClient(cfg.ServiceBusConnectionString, cfg.ServiceBusNamespace)
	if err != nil {
		slog.Error("creating service bus client", "error", err)
		os.Exit(1)
	}

	var sender *servicebus.Sender
	sendDest := cfg.SendTopic
	if sendDest == "" {
		sendDest = cfg.SendQueue
	}
	if sendDest != "" {
		sender, err = servicebus.NewSender(sbClient, sendDest)
		if err != nil {
			slog.Error("creating sender", "error", err)
			os.Exit(1)
		}
		defer sender.Close(context.Background())
	}

	q := queries.New(pool)

	dispatcher := handler.NewDispatcher([]handler.EventHandler{
		handler.NewMachineLocationEventHandler(pool, q, sender),
		handler.NewUserUpdatedEventHandler(),
	})

	// Build unique topic configs from handlers
	seen := make(map[string]bool)
	var topics []servicebus.TopicConfig
	for _, h := range dispatcher.Handlers() {
		key := h.Topic() + "/" + h.Subscription()
		if seen[key] {
			continue
		}
		seen[key] = true
		topics = append(topics, servicebus.TopicConfig{
			Topic:        h.Topic(),
			Subscription: h.Subscription(),
			Strategy:     h.Strategy(),
		})
	}

	healthServer := health.NewServer(cfg.HealthPort, pool)
	go func() {
		if err := healthServer.Start(); err != nil {
			slog.Error("health server error", "error", err)
		}
	}()

	consumer := servicebus.NewConsumer(
		sbClient, cfg.BatchSize, cfg.ConsumerCount, topics,
		func(ctx context.Context, strategy string, receiver *azservicebus.Receiver, messages []*azservicebus.ReceivedMessage) {
			if strategy == "batch" {
				dispatcher.DispatchBatch(ctx, receiver, messages)
			} else {
				dispatcher.DispatchSingle(ctx, receiver, messages)
			}
		},
	)

	slog.Info("starting consumer",
		"consumers", cfg.ConsumerCount,
		"batchSize", cfg.BatchSize,
		"topics", len(topics),
	)

	if err := consumer.Run(ctx); err != nil {
		slog.Error("consumer error", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("health server shutdown error", "error", err)
	}

	slog.Info("shutdown complete")
}
