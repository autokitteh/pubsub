package pubsubfactory

import (
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/autokitteh/pubsub"
	"github.com/autokitteh/pubsub/pubsubinmem"
	"github.com/autokitteh/pubsub/pubsubredis"
)

type RedisConfig struct {
	Address string `envconfig:"ADDRESS" default:"127.0.0.1:6379" json:"address"`
}

type InMemConfig struct {
	QueueSize int `envconfig:"QUEUE_SIZE" json:"queue_size"`
}

type Config struct {
	Type string `envconfig:"TYPE" json:"type"`

	InMem InMemConfig `envconfig:"INMEM" json:"inmem"`
	Redis RedisConfig `envconfig:"REDIS" json:"redis"`
}

func ParseConfig(text string) (*Config, error) {
	t, v, _ := strings.Cut(text, ":")
	if t == "inmem" || t == "" {
		return &Config{Type: t}, nil
	}

	if t == "redis" {
		return &Config{Type: t, Redis: RedisConfig{Address: v}}, nil
	}

	return nil, fmt.Errorf("unknown config type %q", t)
}

func NewFromString(text string) (pubsub.PubSub, error) {
	cfg, err := ParseConfig(text)
	if err != nil {
		return nil, err
	}

	return NewFromConfig(cfg)
}

func NewFromConfig(cfg *Config) (pubsub.PubSub, error) {
	switch cfg.Type {
	case "", "inmem":
		return pubsubinmem.New(cfg.InMem.QueueSize), nil

	case "redis":
		client := redis.NewClient(&redis.Options{
			Addr: cfg.Redis.Address,
		})

		return pubsubredis.New(client), nil

	default:
		return nil, fmt.Errorf("unknown config type %q", cfg.Type)
	}
}
