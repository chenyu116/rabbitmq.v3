package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"io/ioutil"
	"time"
)

type kind string

const (
	KindDirect  kind = amqp.ExchangeDirect
	KindFanout  kind = amqp.ExchangeFanout
	KindHeaders kind = amqp.ExchangeHeaders
	KindTopic   kind = amqp.ExchangeTopic
)

type QueueOption func(queue *queue)

func QueueDurable() QueueOption {
	return func(queue *queue) {
		queue.Durable = true
	}
}
func QueueConsumeAutoAck() QueueOption {
	return func(queue *queue) {
		queue.AutoAck = true
	}
}

func QueueAutoDelete() QueueOption {
	return func(queue *queue) {
		queue.AutoDelete = true
	}
}
func QueueExclusive() QueueOption {
	return func(queue *queue) {
		queue.Exclusive = true
	}
}
func QueueNoWait() QueueOption {
	return func(queue *queue) {
		queue.NoWait = true
	}
}

func QueueArgs(args amqp.Table) QueueOption {
	return func(queue *queue) {
		queue.Args = args
	}
}

type ExchangeOption func(ex *exchange)

func ExchangeDurable() ExchangeOption {
	return func(ex *exchange) {
		ex.Durable = true
	}
}
func ExchangeAutoDelete() ExchangeOption {
	return func(ex *exchange) {
		ex.AutoDelete = true
	}
}
func ExchangeInternal() ExchangeOption {
	return func(ex *exchange) {
		ex.Internal = true
	}
}
func ExchangeNoWait() ExchangeOption {
	return func(ex *exchange) {
		ex.NoWait = true
	}
}

func ExchangeArgs(args amqp.Table) ExchangeOption {
	return func(ex *exchange) {
		ex.Args = args
	}
}

type Option func(cfg *Config)

// Debug display debug log
func Debug() Option {
	return func(cfg *Config) {
		cfg.log.SetLevel(log.DebugLevel)
	}
}

// Auth set rabbitmq login username and password
func Auth(username, password string) Option {
	return func(cfg *Config) {
		cfg.Username = username
		cfg.Password = password
	}
}

// DefaultConsumer add default consumer to queue
func DefaultConsumer() Option {
	return func(cfg *Config) {
		cfg.Consumer = func(_ *Client, msg amqp.Delivery) {
			msg.Ack(false)
		}
	}
}

// Consumer set queue consumer
func Consumer(consumer func(c *Client, msg amqp.Delivery)) Option {
	return func(cfg *Config) {
		cfg.Consumer = consumer
	}
}

// ConsumeInOrder consume message in order
func ConsumeInOrder() Option {
	return func(cfg *Config) {
		cfg.ConsumeInOrder = true
	}
}

func PrefetchCount(count int) Option {
	return func(cfg *Config) {
		cfg.PrefetchCount = count
	}
}

func Recovery(retryInterval time.Duration) Option {
	return func(cfg *Config) {
		cfg.Recovery.Interval = retryInterval
	}
}

// Exchange add exchange,support multiple
func Exchange(name string, kind kind, opts ...ExchangeOption) Option {
	return func(cfg *Config) {
		ex := &exchange{
			Name: name,
			Kind: kind,
		}
		for _, o := range opts {
			o(ex)
		}
		cfg.Exchanges = append(cfg.Exchanges, ex)
	}
}

// Queue set queue properties
func Queue(name, routeKey string, opts ...QueueOption) Option {
	return func(cfg *Config) {
		cfg.Queue = &queue{
			Name:     name,
			RouteKey: routeKey,
		}
		for _, o := range opts {
			o(cfg.Queue)
		}
	}
}

// Confirm Set channel confirm
func Confirm(chSize int, timeout time.Duration, noWait bool) Option {
	return func(cfg *Config) {
		cfg.Confirm.ChSize = chSize
		cfg.Confirm.NoWait = noWait
		cfg.Confirm.Timeout = timeout
	}
}

// QueueDisable do not create queue
func QueueDisable() Option {
	return func(cfg *Config) {
		cfg.QueueDisable = true
	}
}
func VHost(vh string) Option {
	return func(cfg *Config) {
		cfg.Amqp.Vhost = vh
	}
}

func Heartbeat(ht time.Duration) Option {
	return func(cfg *Config) {
		cfg.Amqp.Heartbeat = ht
	}
}

func Tls(tls *tls.Config) Option {
	return func(cfg *Config) {
		cfg.Amqp.TLSClientConfig = tls
	}
}

func AmqpConfig(config amqp.Config) Option {
	return func(cfg *Config) {
		cfg.Amqp = config
	}
}

func TlsCert(caFile, certFile, keyFile, keyFilePassword string) Option {
	return func(cfg *Config) {
		t := new(tls.Config)
		t.RootCAs = x509.NewCertPool()
		if ca, err := ioutil.ReadFile(caFile); err == nil {
			t.RootCAs.AppendCertsFromPEM(ca)
		}
		if keyFilePassword != "" {
			if keyIn, err := ioutil.ReadFile(keyFile); err == nil {
				// Decode and decrypt our PEM block
				decodedPEM, _ := pem.Decode([]byte(keyIn))
				if decrypedPemBlock, err := x509.DecryptPEMBlock(decodedPEM, []byte(keyFilePassword)); err == nil {
					if cert, err := tls.LoadX509KeyPair(certFile, string(decrypedPemBlock)); err == nil {
						t.Certificates = append(t.Certificates, cert)
					}
				}
			}
		} else {
			if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
				t.Certificates = append(t.Certificates, cert)
			}
		}
		cfg.Amqp.TLSClientConfig = t
	}
}

type PublishOption func(pub *amqp.Publishing)

// PublishTimestamp message timestamp
func PublishTimestamp(timestamp time.Time) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Timestamp = timestamp
	}
}

// PublishAppId creating application id
func PublishAppId(appId string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.AppId = appId
	}
}

// PublishUserId creating user id - ex: "guest"
func PublishUserId(userId string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.UserId = userId
	}
}

// PublishType message type name
func PublishType(typ string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Type = typ
	}
}

// PublishReplyTo address to to reply to (ex: RPC)
func PublishReplyTo(replyTo string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.ReplyTo = replyTo
	}
}

// PublishMessageId message identifier
func PublishMessageId(messageId string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.MessageId = messageId
	}
}

// PublishContentEncoding MIME content encoding
func PublishContentEncoding(contentEncoding string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.ContentEncoding = contentEncoding
	}
}

// PublishCorrelationId correlation identifier
func PublishCorrelationId(correlationId string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.CorrelationId = correlationId
	}
}

// PublishContentType         MIME content type
func PublishContentType(contentType string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.ContentType = contentType
	}
}

// PublishExpiration Expiration         // message expiration spec
func PublishExpiration(expire string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Expiration = expire
	}
}

// PublishBody The application specific payload of the message
func PublishBody(body []byte) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Body = body
	}
}

// PublishHeaders Application or exchange specific fields,
// the headers exchange will inspect this field.
func PublishHeaders(keyPairs ...interface{}) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Headers = make(map[string]interface{})
		keyPairsLen := len(keyPairs)
		if keyPairsLen%2 == 1 {
			keyPairs = append(keyPairs, "")
			keyPairsLen++
		}
		for i := 0; i < keyPairsLen; i += 2 {
			pub.Headers[fmt.Sprintf("%v", keyPairs[i])] = keyPairs[i+1]
		}
	}
}

// PublishPriority 0 to 9
func PublishPriority(priority uint8) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Priority = priority
	}
}

// PublishDeliveryMode Transient (0 or 1) or Persistent (2)
func PublishDeliveryMode(deliveryMode uint8) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.DeliveryMode = deliveryMode
	}
}
