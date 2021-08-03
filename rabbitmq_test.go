package rabbitmq

import (
	"github.com/streadway/amqp"
	"testing"
	"time"
)

//func TestClient_Publish(t *testing.T) {
//	_, err := New("192.168.1.2:5672",
//		Auth("guest", "guest"),
//		Heartbeat(time.Second*2),
//		Queue("tester", "tester", QueueDurable()),
//		Exchange("amq.direct", KindDirect, ExchangeDurable()),
//		Consumer(func(msg amqp.Delivery) {
//			msg.Ack(false)
//		}),
//		Confirm(1, time.Second*3, false),
//		VHost("dash-dbvd"),
//	)
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	c, err := New("192.168.1.2:5672",
//		Auth("guest", "guest"),
//		Heartbeat(time.Second*2),
//		Queue("tester1", "tester1", QueueDurable()),
//		Exchange("amq.direct", KindDirect, ExchangeDurable()),
//		//Consumer(func(msg amqp.Delivery) {
//		//	t.Fatalf("msg: %s", msg.Body)
//		//	msg.Ack(false)
//		//}),
//		VHost("dash-dbvd"),
//	)
//	if err != nil {
//		t.Fatal(err)
//	}
//	_ = c
//
//	err = c.Publish("amq.direct", "tester", PublishBody([]byte("tester body")), PublishReplyTo("grpc"))
//	if err != nil {
//		t.Fatal(err)
//	}
//}

func TestClient_PublishDefinite(t *testing.T) {
	_, err := New("192.168.1.2:5672",
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester", "tester", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		Consumer(func(c *Client, msg amqp.Delivery) {
			msg.Ack(false)
			t.Logf("[Consumer]tester msg: %+v", msg)
			if msg.Headers != nil {
				id, ok := msg.Headers["x-definite"]
				from, ok1 := msg.Headers["x-definite-from"]
				if ok && ok1 {
					c.Publish("amq.direct", from.(string), PublishHeaders("x-re-definite", id))
				}
			}

			//t.Fatalf("[Consumer]tester msg: %+v", msg)

		}),
		Confirm(1, time.Second*3, false),
		VHost("dash-dbvd"),
	)
	if err != nil {
		t.Fatal(err)
	}

	c, err := New("192.168.1.2:5672",
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester1", "tester1", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		Consumer(func(c *Client, msg amqp.Delivery) {
			msg.Ack(false)
			t.Logf("[Consumer]tester1 msg: %+v", msg)
		}),
		VHost("dash-dbvd"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = c

	for i := 0; i < 100; i++ {
		err = c.PublishDefinite("amq.direct", "tester", PublishBody([]byte("tester body")))
		if err != nil {
			t.Fatal(err)
		}
	}

}
