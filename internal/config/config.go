package config

import (
	"fmt"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	ServiceBusConnectionString string `env:"SERVICEBUS_CONNECTION_STRING"`
	ServiceBusNamespace        string `env:"SERVICEBUS_NAMESPACE"`
	Topic                      string `env:"SERVICEBUS_TOPIC,required"`
	Subscription               string `env:"SERVICEBUS_SUBSCRIPTION,required"`
	ConsumerCount              int    `env:"CONSUMER_COUNT" envDefault:"10"`
	BatchSize                  int    `env:"BATCH_SIZE" envDefault:"20"`
	HealthPort                 int    `env:"HEALTH_PORT" envDefault:"8080"`

	DBHost               string `env:"DB_HOST,required"`
	DBUser               string `env:"DB_USER,required"`
	DBPassword           string `env:"DB_PASSWORD,required"`
	DBPort               int    `env:"DB_PORT" envDefault:"5432"`
	DBDatabase           string `env:"DB_DATABASE,required"`
	DBSchema             string `env:"DB_SCHEMA"`
	DBSSLMode            string `env:"DB_SSL_MODE" envDefault:"require"`
	DBSimpleProtocol     bool   `env:"DB_SIMPLE_PROTOCOL" envDefault:"false"`
	DBMaxConns           int    `env:"DB_MAX_CONNS" envDefault:"50"`
	DBConnIdleTimeMinutes int   `env:"DB_CONNECTION_IDLE_TIME_MINUTES" envDefault:"5"`
	DBConnLifeTimeMinutes int   `env:"DB_CONNECTION_LIFE_TIME_MINUTES" envDefault:"30"`

	SendTopic string `env:"SERVICEBUS_SEND_TOPIC"`
	SendQueue string `env:"SERVICEBUS_SEND_QUEUE"`
}

func (c *Config) DatabaseURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBDatabase, c.DBSSLMode)
}

func Load() (*Config, error) {
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}
