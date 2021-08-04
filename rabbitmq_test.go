package rabbitmq

import (
	"github.com/streadway/amqp"
	"testing"
	"time"
)

const (
	addr = "192.168.101.232:5672"
)

func TestClient_Publish(t *testing.T) {
	c1, err := New(addr,
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester", "tester", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		Consumer(func(c *Client, msg amqp.Delivery) {
			msg.Ack(false)
		}),
		Confirm(1, time.Second*3, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := New(addr,
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester1", "tester1", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		Consumer(func(c *Client, msg amqp.Delivery) {
			msg.Ack(false)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = c2.Publish("amq.direct", "tester", PublishBody([]byte("tester1 body")))
	if err != nil {
		t.Fatal(err)
	}
	c1.Close()
	c2.Close()
}

func TestClient_PublishDefinite(t *testing.T) {
	_, err := New(addr,
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester", "tester", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		Consumer(func(c *Client, msg amqp.Delivery) {
			msg.Ack(false)
			if msg.Headers != nil {
				id, ok := msg.Headers["x-definite"]
				from, ok1 := msg.Headers["x-definite-from"]
				if ok && ok1 {
					c.Publish("amq.direct", from.(string), PublishHeaders("x-re-definite", id))
				}
			}
		}),
		Confirm(1, time.Second*3, false),
	)
	if err != nil {
		t.Fatal(err)
	}

	c, err := New(addr,
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester1", "tester1", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		Consumer(func(c *Client, msg amqp.Delivery) {
			msg.Ack(false)
		}),
		Confirm(1, time.Second*3, false),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = c.PublishDefinite("amq.direct", "tester", PublishBody([]byte("tester1 body")))
	if err != nil {
		t.Fatal(err)
	}

}
