package rabbitmq

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"time"
)

type exchange struct {
	Name                                  string
	Kind                                  kind
	Durable, AutoDelete, Internal, NoWait bool
	Args                                  amqp.Table
}

type recovery struct {
	Interval  time.Duration
	InProcess bool
	Started   bool
}

type confirm struct {
	ChSize int
	NoWait bool
}

type aqueue struct {
	Name, RouteKey                                  string
	Durable, AutoDelete, Exclusive, NoWait, AutoAck bool
	Args                                            amqp.Table
}

type QueueOption func(queue *aqueue)

func QueueDurable() QueueOption {
	return func(queue *aqueue) {
		queue.Durable = true
	}
}
func QueueAutoDelete() QueueOption {
	return func(queue *aqueue) {
		queue.AutoDelete = true
	}
}
func QueueExclusive() QueueOption {
	return func(queue *aqueue) {
		queue.Exclusive = true
	}
}
func QueueNoWait() QueueOption {
	return func(queue *aqueue) {
		queue.NoWait = true
	}
}

func QueueArgs(args amqp.Table) QueueOption {
	return func(queue *aqueue) {
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

func Auth(username, password string) Option {
	return func(cfg *Config) {
		cfg.Username = username
		cfg.Password = password
	}
}
func Consumer(consumer func(msg amqp.Delivery)) Option {
	return func(cfg *Config) {
		cfg.Consumer = consumer
	}
}

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
func Queue(name, routeKey string, opts ...QueueOption) Option {
	return func(cfg *Config) {
		cfg.Queue = &aqueue{
			Name:     name,
			RouteKey: routeKey,
		}
		for _, o := range opts {
			o(cfg.Queue)
		}
		log.Printf("%+v", cfg.Queue)
	}
}

// Confirm Set channel confirm
func Confirm(chSize int, noWait bool) Option {
	return func(cfg *Config) {
		cfg.Confirm.ChSize = chSize
		cfg.Confirm.NoWait = noWait
	}
}

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

func PublishExpiration(expire string) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Expiration = expire
	}
}

func PublishHeaders(headers amqp.Table) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Headers = headers
	}
}

func PublishPriority(priority uint8) PublishOption {
	return func(pub *amqp.Publishing) {
		pub.Priority = priority
	}
}
