package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MikaelEdebro/servicebus-ingester/internal/config"
	"github.com/MikaelEdebro/servicebus-ingester/internal/db"
	"github.com/MikaelEdebro/servicebus-ingester/internal/db/queries"
	"github.com/MikaelEdebro/servicebus-ingester/internal/handler"
	"github.com/MikaelEdebro/servicebus-ingester/internal/health"
	"github.com/MikaelEdebro/servicebus-ingester/internal/servicebus"
	"github.com/MikaelEdebro/servicebus-ingester/internal/tracing"
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

	tp, err := tracing.Init(ctx, "servicebus-ingester")
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
	h := handler.New(q, sender)

	healthServer := health.NewServer(cfg.HealthPort, pool)
	go func() {
		if err := healthServer.Start(); err != nil {
			slog.Error("health server error", "error", err)
		}
	}()

	consumer := servicebus.NewConsumer(sbClient, cfg.Topic, cfg.Subscription, cfg.BatchSize, cfg.ConsumerCount, h.HandleLoadtestListen)

	slog.Info("starting consumer",
		"topic", cfg.Topic,
		"subscription", cfg.Subscription,
		"consumers", cfg.ConsumerCount,
		"batchSize", cfg.BatchSize,
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
