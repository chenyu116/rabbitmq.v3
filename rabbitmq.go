package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	xDefinite = "x-definite"

	defaultSendTimeout = time.Second * 10
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:   true,
		DisableColors: true,
		FullTimestamp: true,
	})
}

type Client struct {
	config             *Config
	conn               *amqp.Connection
	channel            *amqp.Channel
	confirmChan        chan amqp.Confirmation
	definiteMessageMap map[string]chan struct{}
	definiteMu         sync.Mutex
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
	go c.consume()
	go c.recovery()
	return
}

func (c *Client) connect() (err error) {
	scheme := "amqp"
	if c.config.Amqp.TLSClientConfig != nil {
		scheme = "amqps"
	}
	rawURL := fmt.Sprintf("%s://%s:%s@%s/", scheme, c.config.Username, c.config.Password, c.config.Addr)
	c.conn, err = amqp.DialConfig(rawURL, c.config.Amqp)
	return
}

func (c *Client) init() (err error) {
	entry := c.config.log.WithFields(log.Fields{
		"prod":   "rabbitmq.v3",
		"method": fmt.Sprintf("queue(%s) init", c.config.Queue.Name),
	})
	c.channel, err = c.conn.Channel()
	if err != nil {
		return
	}
	if c.config.Confirm.ChSize > 0 {
		entry.Debug("channel.NotifyPublish")
		c.confirmChan = c.channel.NotifyPublish(make(chan amqp.Confirmation, c.config.Confirm.ChSize))
		err = c.channel.Confirm(c.config.Confirm.NoWait)
		if err != nil {
			return
		}
	}
	if c.config.Queue.Name != "" {
		if c.config.Qos.PrefetchCount < 0 {
			c.config.Qos.PrefetchCount = 0
		}
		err = c.channel.Qos(c.config.Qos.PrefetchCount, c.config.Qos.PrefetchSize, c.config.Qos.Global)
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

func (c *Client) consume() {
	entry := c.config.log.WithFields(log.Fields{
		"prod":   "rabbitmq.v3",
		"method": fmt.Sprintf("queue(%s) consume", c.config.Queue.Name),
	})
	if c.config.Queue.Name == "" {
		entry.Debug("no queue,consume will not start")
		return
	}
	defer func() {
		entry.Debug("stopped")
	}()
	if c.config.ConsumerTag == "" {
		c.config.ConsumerTag = c.config.Queue.Name
	}
	messages, err := c.channel.Consume(
		c.config.Queue.Name,      // queue
		c.config.ConsumerTag,     // consumer
		c.config.Queue.AutoAck,   // auto-ack
		c.config.Queue.Exclusive, // exclusive
		false,                    // no-local
		c.config.Queue.NoWait,    // no-wait
		c.config.Queue.Args,      // args
	)

	if err != nil {
		entry.Debugf("channel.Consume error(%v)", err)
		return
	}

	entry.Debug("consume messages")
	for msg := range messages {
		d := msg
		entry.Debugf("receive message(%+v)", d)
		if id, ok := d.Headers["x-re-definite"]; ok {
			_ = d.Ack(false)
			go c.definite(id.(string))
			continue
		}

		if c.config.Consumer == nil {
			entry.Debugf("queue(%s) no consumer,continue", c.config.Queue.Name)
			time.Sleep(time.Second)
			continue
		}
		if c.config.ConsumeInOrder {
			c.config.Consumer(c, d)
		} else {
			go c.config.Consumer(c, d)
		}
	}
}

func (c *Client) SetConsumer(consumer func(c *Client, msg amqp.Delivery)) {
	c.config.Consumer = consumer
}

func (c *Client) definite(id string) {
	entry := c.config.log.WithFields(log.Fields{
		"prod":   "rabbitmq.v3",
		"method": fmt.Sprintf("queue(%s) definite", c.config.Queue.Name),
	})
	c.definiteMu.Lock()
	defer c.definiteMu.Unlock()
	if ch, ok := c.definiteMessageMap[id]; ok {
		entry.Debugf("definite message found! id(%s)", id)
		delete(c.definiteMessageMap, id)
		close(ch)
	} else {
		entry.Debugf("definite message not found! id(%s)", id)
	}
}

func (c *Client) recovery() {
	entry := c.config.log.WithFields(log.Fields{
		"prod":   "rabbitmq.v3",
		"method": fmt.Sprintf("queue(%s) recovery", c.config.Queue.Name),
	})
	if c.config.Recovery.Started {
		entry.Debug("already started,return")
		return
	}
	defer entry.Debug("stopped")
	c.config.Recovery.Started = true
	recoveryTicker := time.NewTicker(c.config.Recovery.Interval)
	c.config.Recovery.InProgress = false
	for range recoveryTicker.C {
		if !c.isClosed() || c.config.Recovery.InProgress {
			continue
		}
		entry.Debug("recovery start")
		c.config.Recovery.InProgress = true
		err := c.start()
		if err != nil {
			entry.Debugf("recovery error(%v)", err)
		} else {
			entry.Debug("recovery success")
		}
		entry.Debug("recovery end")
		c.config.Recovery.InProgress = false
	}
}

func (c *Client) Close() {
	if c.isClosed() {
		return
	}
	_ = c.channel.Close()
	_ = c.conn.Close()
	c.conn = nil
}

func (c *Client) isClosed() bool {
	return c.conn == nil || c.conn.IsClosed()
}

func (c *Client) ParseDefiniteMessage(msg amqp.Delivery) (dm DefiniteMessage, ok bool) {
	entry := c.config.log.WithFields(log.Fields{
		"prod":   "rabbitmq.v3",
		"method": fmt.Sprintf("queue(%s) ParseDefiniteMessage", c.config.Queue.Name),
	})
	if msg.Headers == nil {
		return
	}

	if data, ok1 := msg.Headers[xDefinite]; ok1 {
		err := json.Unmarshal(data.([]byte), &dm)
		if err != nil {
			entry.Debugf("parse x-definite error(%v)", err)
			return
		}
		ok = true
	}
	return
}

// Publish send message
func (c *Client) Publish(exchange, routeKey string, opts ...PublishOption) (err error) {
	if c.isClosed() {
		err = errors.New("channel closed")
		return
	}
	entry := c.config.log.WithFields(log.Fields{
		"prod":   "rabbitmq.v3",
		"method": fmt.Sprintf("queue(%s) Publish", c.config.Queue.Name),
	})
	msg := &amqp.Publishing{}
	for _, o := range opts {
		o(msg)
	}
	timeout := defaultSendTimeout
	if c.config.Confirm.Timeout > 0 {
		timeout = c.config.Confirm.Timeout
	}
	if msg.Expiration != "" {
		expiration, e := strconv.Atoi(msg.Expiration)
		if e != nil {
			return e
		}
		if expiration <= 0 {
			return errors.New("expiration invalid")
		}
		timeout = time.Duration(expiration) * time.Millisecond
	}

	entry.Debugf("message(%+v)", msg)

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
		entry.Debug("wait server confirm")
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
	if c.config.Queue.Name == "" {
		err = errors.New("the queue must be set")
		return
	}

	bindExchange := false
	for _, ex := range c.config.Exchanges {
		if ex.Name == exchange {
			bindExchange = true
			break
		}
	}

	if !bindExchange {
		err = errors.New("the same exchange needed")
		return
	}
	entry := c.config.log.WithFields(log.Fields{
		"prod":   "rabbitmq.v3",
		"method": fmt.Sprintf("queue(%s) PublishDefinite", c.config.Queue.Name),
	})
	msg := &amqp.Publishing{}
	for _, o := range opts {
		o(msg)
	}
	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}

	id := uuid.NewString()

	dm := DefiniteMessage{
		Id:       id,
		From:     c.config.Queue.RouteKey,
		Exchange: exchange,
	}

	msg.Headers[xDefinite], err = json.Marshal(dm)
	if err != nil {
		return
	}

	timeout := defaultSendTimeout
	if c.config.Confirm.Timeout > 0 {
		timeout = c.config.Confirm.Timeout
	}

	if msg.Expiration != "" {
		expiration, e := strconv.Atoi(msg.Expiration)
		if e != nil {
			return e
		}

		if expiration <= 0 {
			return errors.New("expiration invalid")
		}

		timeout = time.Duration(expiration) * time.Millisecond
	} else {
		now := time.Now()
		seconds := now.Add(timeout).Sub(now).Seconds() * 1000
		msg.Expiration = fmt.Sprintf("%v", seconds)
	}

	entry.Debugf("message(%+v)", msg)

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
		entry.Debug("wait server confirm")
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

	definiteCh := make(chan struct{})
	c.definiteMu.Lock()
	c.definiteMessageMap[id] = definiteCh
	c.definiteMu.Unlock()
	entry.Debugf("waiting for confirm message! id(%s)", id)
	select {
	case <-definiteCh:
	case <-time.After(timeout):
		err = fmt.Errorf("definite message timeout! id:%s", id)
	}
	return
}

func New(addr string, options ...Option) (*Client, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	cfg := NewConfig()
	cfg.log.SetLevel(log.InfoLevel)
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	for _, o := range options {
		o(cfg)
	}

	if cfg.Recovery.Interval == 0 {
		cfg.Recovery.Interval = time.Second * 6
	}
	c := &Client{config: cfg, definiteMessageMap: make(map[string]chan struct{})}
	err = c.start()
	if err != nil {
		return nil, err
	}

	return c, nil
}
