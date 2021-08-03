package rabbitmq

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"sync"
	"time"
)

type Client struct {
	config      *Config
	conn        *amqp.Connection
	channel     *amqp.Channel
	confirmChan chan amqp.Confirmation
	definiteMap map[string]chan struct{}
	definiteMu  sync.Mutex
}

func (c *Client) start() (err error) {
	err = c.connect()
	if err != nil {
		return
	}
	err = c.init()
	if err != nil {
		return
	}
	go c.startConsumers()
	go c.recovery()
	return
}

func (c *Client) connect() (err error) {
	scheme := "amqp"
	if c.config.Amqp.TLSClientConfig != nil {
		scheme = "amqps"
	}
	rawURL := fmt.Sprintf("%s://%s:%s@%s/", scheme, c.config.Username, c.config.Password, c.config.HostPort)
	c.conn, err = amqp.DialConfig(rawURL, c.config.Amqp)
	return
}

func (c *Client) init() (err error) {
	c.channel, err = c.conn.Channel()
	if err != nil {
		return
	}
	if c.config.Confirm.ChSize > 0 {
		c.confirmChan = c.channel.NotifyPublish(make(chan amqp.Confirmation, c.config.Confirm.ChSize))
		err = c.channel.Confirm(c.config.Confirm.NoWait)
		if err != nil {
			return
		}
	}
	if c.config.QueueDisable == false {
		if c.config.PrefetchCount <= 0 {
			c.config.PrefetchCount = 1
		}
		err = c.channel.Qos(c.config.PrefetchCount, 0, false)
		if err != nil {
			return
		}
		_, err = c.channel.QueueDeclare(
			c.config.Queue.Name,       // name
			c.config.Queue.Durable,    // durable
			c.config.Queue.AutoDelete, // delete when unused
			c.config.Queue.Exclusive,  // exclusive
			c.config.Queue.NoWait,     // no-wait
			c.config.Queue.Args,       // arguments
		)
		if err != nil {
			return
		}

		for _, v := range c.config.Exchanges {
			err = c.channel.ExchangeDeclare(v.Name, string(v.Kind), v.Durable, v.AutoDelete, v.Internal, v.NoWait, v.Args)
			if err != nil {
				return
			}
			err = c.channel.QueueBind(
				c.config.Queue.Name,     // queue name
				c.config.Queue.RouteKey, // routing key
				v.Name,                  // exchange
				c.config.Queue.NoWait,
				c.config.Queue.Args,
			)
			if err != nil {
				return
			}
		}
	}
	return
}

func (c *Client) startConsumers() {
	if c.config.Consumer == nil {
		log.Println("[rabbitmq.v3] no consumer found")
		return
	}
	log.Println("startConsumers started")
	defer func() {
		log.Println("startConsumers stopped")
	}()

	messages, err := c.channel.Consume(
		c.config.Queue.Name,      // queue
		c.config.Queue.Name,      // consumer
		c.config.Queue.AutoAck,   // auto-ack
		c.config.Queue.Exclusive, // exclusive
		false,                    // no-local
		c.config.Queue.NoWait,    // no-wait
		c.config.Queue.Args,      // args
	)

	if err != nil {
		return
	}

	for msg := range messages {
		d := msg
		if id, ok := d.Headers["x-re-definite"]; ok {
			log.Println("id", id)
			_ = d.Ack(false)
			go c.definite(id.(string))
			continue
		}
		if c.config.ConsumeInOrder {
			c.config.Consumer(c, d)
		} else {
			go c.config.Consumer(c, d)
		}
	}
}

func (c *Client) definite(id string) {
	c.definiteMu.Lock()
	defer c.definiteMu.Unlock()
	if ch, ok := c.definiteMap[id]; ok {
		delete(c.definiteMap, id)
		close(ch)
	}
}
func (c *Client) recovery() {
	if c.config.Recovery.Started {
		return
	}
	c.config.Recovery.Started = true
	recoveryTicker := time.NewTicker(c.config.Recovery.Interval)
	c.config.Recovery.InProcess = false
	for range recoveryTicker.C {
		if !c.isClosed() || c.config.Recovery.InProcess {
			continue
		}
		c.config.Recovery.InProcess = true
		log.Println("start recovery")
		err := c.start()
		if err != nil {
			log.Println(err)
		}
		c.config.Recovery.InProcess = false
	}
}

func (c *Client) isClosed() bool {
	return c.conn == nil || c.conn.IsClosed()
}

func (c *Client) Publish(exchange, routeKey string, opts ...PublishOption) (err error) {
	if c.isClosed() {
		err = errors.New("channel closed")
		return
	}
	msg := &amqp.Publishing{}
	for _, o := range opts {
		o(msg)
	}
	timeout := time.Second * 10
	if c.config.Confirm.Timeout > 0 {
		timeout = c.config.Confirm.Timeout
	}
	if msg.Expiration != "" {
		expiration, err := strconv.Atoi(msg.Expiration)
		if err == nil {
			_timeout := time.Duration(expiration) * time.Millisecond
			if _timeout > 0 {
				timeout = _timeout
			}
		}
	}

	err = c.channel.Publish(
		exchange,
		routeKey,
		false,
		false,
		*msg)
	if err != nil {
		return
	}

	if c.config.Confirm.ChSize > 0 {
		select {
		case <-time.After(timeout):
			err = errors.New("publish timeout")
			return
		case m := <-c.confirmChan:
			if !m.Ack {
				err = errors.New("publish nack")
				return
			}
		}
	}
	return
}

// PublishDefinite send definite message
func (c *Client) PublishDefinite(exchange, routeKey string, opts ...PublishOption) (err error) {
	if c.isClosed() {
		err = errors.New("channel closed")
		return
	}
	msg := &amqp.Publishing{}
	for _, o := range opts {
		o(msg)
	}
	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}

	id := uuid.NewString()

	msg.Headers["x-definite"] = id
	msg.Headers["x-definite-from"] = c.config.Queue.Name
	//if !c.config.QueueEnable {
	//	confirm = false
	//}
	//if confirm && msg.ReplyTo == "" {
	//	err = errors.New("ReplyTo not defined")
	//	return
	//}
	timeout := time.Second * 10
	if c.config.Confirm.Timeout > 0 {
		timeout = c.config.Confirm.Timeout
	}
	//if msg.AppId == "" {
	//	msg.AppId = c.queueName
	//}
	//
	if msg.Expiration != "" {
		expiration, err := strconv.Atoi(msg.Expiration)
		if err == nil {
			_timeout := time.Duration(expiration) * time.Millisecond
			if _timeout > 0 {
				timeout = _timeout
			}
		}
	}
	//
	err = c.channel.Publish(
		exchange,
		routeKey,
		false,
		false,
		*msg)
	if err != nil {
		return
	}

	if c.config.Confirm.ChSize > 0 {
		select {
		case <-time.After(timeout):
			err = errors.New("publish timeout")
			return
		case m := <-c.confirmChan:
			if !m.Ack {
				err = errors.New("publish nack")
				return
			}
		}
	}

	definiteCh := make(chan struct{}, 1)
	c.definiteMu.Lock()
	c.definiteMap[id] = definiteCh
	c.definiteMu.Unlock()

	select {
	case <-definiteCh:
	case <-time.After(timeout):
		err = fmt.Errorf("wait for definite message timeout! id:%s", id)
	}
	return
}

func New(hostport string, options ...Option) (*Client, error) {
	cfg := new(Config)
	cfg.HostPort = hostport
	for _, o := range options {
		o(cfg)
	}

	if cfg.Recovery.Interval == 0 {
		cfg.Recovery.Interval = time.Second * 6
	}
	c := &Client{config: cfg, definiteMap: make(map[string]chan struct{})}
	err := c.start()
	if err != nil {
		return nil, err
	}
	return c, nil
}
