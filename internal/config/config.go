package config

import (
	"github.com/caarlos0/env/v11"
)

type Config struct {
	ServiceBusConnectionString string `env:"SERVICEBUS_CONNECTION_STRING"`
	ServiceBusNamespace        string `env:"SERVICEBUS_NAMESPACE"`
	Topic                      string `env:"SERVICEBUS_TOPIC,required"`
	Subscription               string `env:"SERVICEBUS_SUBSCRIPTION,required"`
	DatabaseURL                string `env:"DATABASE_URL,required"`
	DatabaseSchema             string `env:"DATABASE_SCHEMA"`
	ConsumerCount              int    `env:"CONSUMER_COUNT" envDefault:"10"`
	BatchSize                  int    `env:"BATCH_SIZE" envDefault:"20"`
	HealthPort                 int    `env:"HEALTH_PORT" envDefault:"8080"`

	SendTopic string `env:"SERVICEBUS_SEND_TOPIC"`
	SendQueue string `env:"SERVICEBUS_SEND_QUEUE"`
}

func Load() (*Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
