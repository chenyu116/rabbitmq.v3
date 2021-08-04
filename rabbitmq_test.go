package rabbitmq

import (
	"github.com/streadway/amqp"
	"testing"
	"time"
)

const (
	addr = "192.168.101.232:5672"
)

//func TestClient_Publish(t *testing.T) {
//	c1, err := New(addr,
//		Auth("guest", "guest"),
//		Heartbeat(time.Second*2),
//		Queue("tester", "tester", QueueDurable()),
//		Exchange("amq.direct", KindDirect, ExchangeDurable()),
//		Debug(),
//		Consumer(func(c *Client, msg amqp.Delivery) {
//			msg.Ack(false)
//			t.Log("Old Consumer")
//		}),
//	)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	c2, err := New(addr,
//		Auth("guest", "guest"),
//		Heartbeat(time.Second*2),
//	)
//	if err != nil {
//		t.Fatal(err)
//	}
//	err = c2.Publish("amq.direct", "tester", PublishBody([]byte("tester1 body")))
//	if err != nil {
//		t.Fatal(err)
//	}
//	_ = c1
//	_ = c2
//	c1.Close()
//	c2.Close()
//}

//func TestClient_PublishDefinite(t *testing.T) {
//	_, err := New(addr,
//		Auth("guest", "guest"),
//		Heartbeat(time.Second*2),
//		Queue("tester", "tester", QueueDurable()),
//		Exchange("amq.direct", KindDirect, ExchangeDurable()),
//		Consumer(func(c *Client, msg amqp.Delivery) {
//			dm, ok := c.ParseDefiniteMessage(msg)
//			if ok {
//				c.Publish(dm.Exchange, dm.From, PublishHeaders("x-re-definite", dm.Id))
//			}
//			msg.Ack(false)
//		}),
//	)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	c, err := New(addr,
//		Auth("guest", "guest"),
//		Queue("sender", "sender", QueueAutoDelete()),
//		Exchange("amq.direct", KindDirect, ExchangeDurable()),
//		Heartbeat(time.Second*2),
//		DefaultConsumer(),
//	)
//	if err != nil {
//		t.Fatal(err)
//	}
//	err = c.PublishDefinite("amq.direct", "tester", PublishBody([]byte("tester1 body")))
//	if err != nil {
//		t.Fatal(err)
//	}
//}

func Test_ChangeConsumer(t *testing.T) {
	c1, err := New(addr,
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester", "tester", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		Debug(),
		//Consumer(func(c *Client, msg amqp.Delivery) {
		//	msg.Ack(false)
		//	t.Log("Old Consumer")
		//}),
	)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := New(addr,
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = c2.Publish("amq.direct", "tester", PublishBody([]byte("tester1 body")))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	c1.SetConsumer(func(c *Client, msg amqp.Delivery) {
		msg.Ack(false)
		t.Log("New Consumer")
	})
	time.Sleep(time.Second * 10)
	err = c2.Publish("amq.direct", "tester", PublishBody([]byte("tester1 body")))
	if err != nil {
		t.Fatal(err)
	}
	_ = c1
	_ = c2
	c1.Close()
	c2.Close()
}
