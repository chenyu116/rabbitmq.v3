package rabbitmq

import (
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
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
	log            *log.Logger
	Amqp           amqp.Config
}

func NewConfig() *Config {
	return &Config{
		PrefetchCount:  1,
		Queue:          new(queue),
		QueueDisable:   false,
		ConsumeInOrder: false,
		Confirm: confirm{
			ChSize:  1,
			Timeout: time.Second * 3,
			NoWait:  false,
		},
		log:      log.New(),
		Recovery: recovery{Interval: time.Second * 6},
		Amqp:     amqp.Config{},
	}
}
