package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type client struct {
	config      *Config
	conn        *amqp.Connection
	channel     *amqp.Channel
	confirmChan chan amqp.Confirmation
}

func (c *client) start() (err error) {
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

func (c *client) connect() (err error) {
	scheme := "amqp"
	if c.config.Amqp.TLSClientConfig != nil {
		scheme = "amqps"
	}
	rawURL := fmt.Sprintf("%s://%s:%s@%s/", scheme, c.config.Username, c.config.Password, c.config.HostPort)
	c.conn, err = amqp.DialConfig(rawURL, c.config.Amqp)
	return
}

func (c *client) init() (err error) {
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

func (c *client) startConsumers() {
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
		if c.config.ConsumeInOrder {
			c.config.Consumer(d)
		} else {
			go c.config.Consumer(d)
		}
	}
}

func (c *client) recovery() {
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

func (c *client) isClosed() bool {
	return c.conn == nil || c.conn.IsClosed()
}

func (c *client) Publish(exchange string, body []byte, opts ...PublishOption) (err error) {
	pub := &amqp.Publishing{}
	for _, o := range opts {
		o(pub)
	}
	fmt.Println(pub)
	//if c.isClosed() {
	//	err = errors.New("source channel closed")
	//	return
	//}
	//if !c.config.QueueEnable {
	//	confirm = false
	//}
	//if confirm && msg.ReplyTo == "" {
	//	err = errors.New("ReplyTo not defined")
	//	return
	//}
	//timeout := c.config.ReplyConfirmTimeout
	//if msg.AppId == "" {
	//	msg.AppId = c.queueName
	//}
	//
	//if msg.Expiration != "" {
	//	expiration, err := strconv.Atoi(msg.Expiration)
	//	if err == nil {
	//		_timeout := time.Duration(expiration/1000) * time.Second
	//		if _timeout > 0 {
	//			timeout = _timeout
	//		}
	//	}
	//}
	//
	//err = c.channel.Publish(
	//	exchange,
	//	routeKey,
	//	false,
	//	false,
	//	msg)
	//if err != nil {
	//	return
	//}
	//select {
	//case <-time.After(timeout):
	//	err = errors.New("publish fail")
	//	return
	//case m := <-c.confirmChan:
	//	if !m.Ack {
	//		err = errors.New("publish fail")
	//		return
	//	}
	//}
	//
	//if confirm {
	//	ctx, can := context.WithTimeout(context.Background(), timeout)
	//	c.confirmMapMu.Lock()
	//	c.confirmMap[msg.ReplyTo] = can
	//	c.confirmMapMu.Unlock()
	//	<-ctx.Done()
	//	if ctx.Err() == context.Canceled {
	//		return nil
	//	}
	//	err = errors.New("confirmed timeout")
	//	c.confirmMapMu.Lock()
	//	delete(c.confirmMap, msg.ReplyTo)
	//	c.confirmMapMu.Unlock()
	//}
	return
}

func New(hostport string, options ...Option) (*client, error) {
	cfg := new(Config)
	cfg.HostPort = hostport
	for _, o := range options {
		o(cfg)
	}

	if cfg.Recovery.Interval == 0 {
		cfg.Recovery.Interval = time.Second * 6
	}
	c := &client{config: cfg}
	err := c.start()
	if err != nil {
		return nil, err
	}
	return c, nil
}
