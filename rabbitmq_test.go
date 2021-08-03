package rabbitmq

import (
	"github.com/streadway/amqp"
	"testing"
	"time"
)

func Test_New(t *testing.T) {
	c, err := New("192.168.101.232:5672",
		Auth("guest", "guest"),
		Heartbeat(time.Second*2),
		Queue("tester", "tester", QueueDurable()),
		Exchange("amq.direct", KindDirect, ExchangeDurable()),
		//Consumer(func(msg amqp.Delivery) {
		//	t.Fatalf("msg: %s", msg.Body)
		//	msg.Ack(false)
		//}),
		VHost("dash-dbvd"),
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = c
	opts := []PublishOption{PublishExpiration("10000")}
	pub := &amqp.Publishing{}
	for _, o := range opts {
		o(pub)
	}

	t.Fatalf("%+v", *pub)
	//c.Publish("te", []byte("hello"), PublishExpiration("100000"))
	time.Sleep(time.Second * 2)
}

//
//func TestClient_Publish(t *testing.T) {
//  client := NewClient(rbtConfig)
//  err := client.Start()
//  if err != nil {
//    t.Fatal(err)
//  }
//  err = client.Publish("websocketServer-fanout", "", amqp.Publishing{
//    AppId: client.config.QueueName,
//    Body:  []byte("test"),
//  }, nil)
//  if err != nil {
//    t.Fatal(err)
//  }
//}
//
//func TestClient_PublishWithConfirm(t *testing.T) {
//	//
//	rbtConfig1 := NewConfig()
//	rbtConfig1.HostPort = rbtConfig.HostPort
//	rbtConfig1.amqp = amqp.Config{
//		Vhost:     c.Rabbitmq.VHost,
//		Heartbeat: time.Second * 3,
//	}
//	rbtConfig1.Prefetch = rbtConfig.Prefetch
//	rbtConfig1.Username = rbtConfig.Username
//	rbtConfig1.Password = rbtConfig.Password
//	rbtConfig1.Exchanges = rbtConfig.Exchanges
//	rbtConfig1.QueueName = "confirmServer-" + time.Now().String()
//	r := NewClient(rbtConfig1)
//	err := r.Start()
//	if err != nil {
//		t.Fatal(err)
//	}
//	r.AddMessageConsumer(func(msg amqp.Delivery) {
//		//log.Printf("confirmServer message %+v", msg)
//		r.Publish("websocketServer-direct", msg.ReplyTo, amqp.Publishing{
//			Type:    TYPE_REPLY,
//			ReplyTo: msg.ReplyTo,
//			Body:    []byte("confirmed " + msg.AppId),
//		})
//	})
//	//time.Sleep(time.Second * 10)
//	client := NewClient(rbtConfig)
//	err = client.Start()
//	if err != nil {
//		t.Fatal(err)
//	}
//	r.AddMessageConsumer(func(msg amqp.Delivery) {
//		//log.Printf("websocketServer message %+v", msg)
//		r.Publish("websocketServer-direct", msg.ReplyTo, amqp.Publishing{
//			Type:    TYPE_REPLY,
//			ReplyTo: msg.ReplyTo,
//			Body:    []byte("confirmed " + msg.AppId),
//		})
//	})
//	client.AddReplyConsumer(func(msg amqp.Delivery) {
//		fmt.Println("ReplyConsumer", string(msg.Body))
//	})
//	time.Sleep(time.Second * 3)
//
//	//for i := 0; i < 1000; i++ {
//	err = client.Publish("websocketServer-fanout", "confirmServer", amqp.Publishing{
//		Type:    TYPE_SEND,
//		ReplyTo: client.config.QueueName,
//		Body:    []byte("test"),
//	}, Confirm{NeedConfirm: true, Multi: true, Timeout: time.Second * 30})
//	if err != nil {
//		t.Fatal(err)
//	}
//
//	fmt.Println("all confirmed")
//	time.Sleep(time.Hour)
//}

//func TestClient_Recovery(t *testing.T) {
// client := NewClient(rbtConfig)
// err := client.Start()
// if err != nil {
//   t.Fatal(err)
// }
//
// time.Sleep(time.Hour)
//}
