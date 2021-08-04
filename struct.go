package rabbitmq

import (
	"github.com/streadway/amqp"
	"time"
)

type exchange struct {
	Name                                  string
	Kind                                  kind
	Durable, AutoDelete, Internal, NoWait bool
	Args                                  amqp.Table
}

type recovery struct {
	Interval            time.Duration
	InProgress, Started bool
}

type confirm struct {
	ChSize  int
	Timeout time.Duration
	NoWait  bool
}

type queue struct {
	Name, RouteKey                                  string
	Durable, AutoDelete, Exclusive, NoWait, AutoAck bool
	Args                                            amqp.Table
}
