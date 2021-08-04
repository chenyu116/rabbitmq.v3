package rabbitmq

import (
	"github.com/streadway/amqp"
)

type Config struct {
	Addr          string
	Username      string
	Password      string
	PrefetchCount int
	Exchanges     []*exchange
	Queue         *queue
	QueueDisable  bool
	Confirm       confirm
	Consumer      func(c *Client, msg amqp.Delivery)
	// if ConsumeInOrder set to true,consumer will process message in order,not use goroutine
	ConsumeInOrder bool
	Recovery       recovery
	Amqp           amqp.Config
}

func NewConfig() *Config {
	return new(Config)
}
