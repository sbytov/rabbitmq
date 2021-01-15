package anycast

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	shortuuid "github.com/lithammer/shortuuid/v3"
	"github.com/streadway/amqp"
	"github.com/vmihailenco/msgpack"

	"git.acronis.com/abc/go-libs/log"
	"git.acronis.com/abc/go-libs/rabbitmq"
)

// Client is an anycast client.
type Client struct {
	pub         *rabbitmq.Publisher
	sub         *rabbitmq.Consumer
	logger      log.FieldLogger
	queue       string
	clientID    string
	exchange    string
	mu          sync.RWMutex
	subscribers map[string]SubscribeFunc
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

const (
	// DefaultPriority is message priority set to average.
	DefaultPriority = 127
	// DefaultRetryDelay is default timeout between reconnection attempts.
	DefaultRetryDelay = time.Second * 5
)

const (
	defaultPrefix      = "anycast"
	defaultUnroutedTTL = time.Minute * 2
	defaultTTL         = time.Hour * 24 * 24
	pubSubMessageType  = "pub/sub"
)

// Config is anycast client configuration.
type Config struct {
	// Publisher configuration.
	Publisher rabbitmq.PublisherConfig
	// Consumer configuration.
	Consumer rabbitmq.ConsumerConfig
	// RetryDelay specifies the timeout used between connection attempts to server.
	RetryDelay time.Duration
	// PrefetchCount
	PrefetchCount int
}

// SubscribeFunc defines a callback signature for subscriptions.
// For each delivered message it will be called with msg containing unpacked message body.
// If this is a redelivery the second parameter will be true.
type SubscribeFunc func(msg interface{}, redelivery bool)

// NewClient is like NewPrefixClient with default prefix 'anycast'.
//nolint:gocritic
func NewClient(clientID string, rabbitURL *url.URL, cfg Config, durable, autoDelete bool, logger log.FieldLogger) (*Client, error) {
	return NewPrefixClient("", clientID, rabbitURL, cfg, durable, autoDelete, logger)
}

// NewPrefixClient creates a new anycast client.
// It creates necessary RabbitMQ topology and starts consuming in the background.
//nolint:gocritic, gocyclo
func NewPrefixClient(prefix, clientID string, rabbitURL *url.URL, cfg Config,
	durable, autoDelete bool, logger log.FieldLogger) (*Client, error) {
	if prefix == "" {
		prefix = defaultPrefix
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = DefaultRetryDelay
	}
	if clientID == "" {
		clientID = shortuuid.New()
	}
	pub, err := rabbitmq.NewConfiguredPublisher(rabbitURL, cfg.Publisher, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %v", err)
	}
	defer func() {
		if err != nil && pub != nil {
			pub.Close()
		}
	}()
	unrouted := fmt.Sprintf("%v.%v", prefix, "UNROUTED")
	err = pub.ExchangeDeclare(unrouted, rabbitmq.ExchangeHeaders, false, false, false, false,
		amqp.Table{"x-message-ttl": defaultUnroutedTTL.Milliseconds()})
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %q: %v", unrouted, err)
	}
	logger.Debug("declared exchange", log.String("exchange", unrouted))

	exchangeName := fmt.Sprintf("%v.%v", prefix, "PUBSUB")
	err = pub.ExchangeDeclare(exchangeName, rabbitmq.ExchangeDirect, false, false, false, false,
		amqp.Table{"x-message-ttl": defaultTTL.Milliseconds(), "alternate-exchange": unrouted})
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %q: %v", exchangeName, err)
	}
	logger.Debug("declared exchange", log.String("exchange", exchangeName))

	queueName := fmt.Sprintf("pubsub.%v", clientID)
	queue, err := pub.QueueDeclare(queueName, durable, autoDelete, false, false,
		amqp.Table{"x-message-ttl": defaultTTL.Milliseconds(), "x-expires": defaultTTL.Milliseconds() * 2})
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %q: %v", queueName, err)
	}
	logger.Debug("declared queue", log.String("queue", queueName))

	err = pub.QueueBind(queue.Name, clientID, exchangeName, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue %q to exchange %q: %v", queue.Name, exchangeName, err)
	}
	logger.Debug("bound queue to exchange", log.String("queue", queueName), log.String("exchange", exchangeName))

	sub, err := rabbitmq.NewConfiguredConsumer(rabbitURL, cfg.Consumer, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	a := &Client{
		pub:         pub,
		sub:         sub,
		queue:       queue.Name,
		exchange:    exchangeName,
		logger:      logger,
		clientID:    clientID,
		subscribers: make(map[string]SubscribeFunc),
		cancel:      cancel,
	}

	a.wg.Add(1)
	go func() {
		// The following infinite loop is necessary because consume may return an error if connection/channel gets closed
		// while there are still pending acknowledgments.
		// If this happens the non-acknowledged messages will be redelivered so subscribers should be idempotent to be safe.
		for {
			err := a.consume(ctx, cfg.RetryDelay, cfg.PrefetchCount)
			if err != nil {
				a.logger.Error("failed to consume from queue", log.String("queue", a.queue), log.Error(err))
			}
			select {
			case <-ctx.Done():
				a.wg.Done()
				return
			case <-time.After(cfg.RetryDelay):
				a.logger.Debugf("reconsume after %v", cfg.RetryDelay)
			}
		}
	}()
	return a, nil
}

// Close cancels consuming, closes channels and connections.
func (a *Client) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cancel()
	a.wg.Wait()
	for ch := range a.subscribers {
		err := a.sub.QueueUnbind(a.queue, ch, a.exchange, nil)
		if err != nil {
			a.logger.Error("failed to unbind queue", log.String("queue", a.queue), log.String("channel", ch), log.Error(err))
		}
		delete(a.subscribers, ch)
	}
	a.sub.Close()
	a.pub.Close()
}

// Subscribe registers a new subscriber listening to channel.
func (a *Client) Subscribe(channel string, fn SubscribeFunc) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.subscribers[channel]; ok {
		return fmt.Errorf("channel already listened to")
	}
	err := a.sub.QueueBind(a.queue, channel, a.exchange, false, nil)
	if err != nil {
		return fmt.Errorf("failed to bind queue %q to exchange %q: %v", a.queue, a.exchange, err)
	}
	a.subscribers[channel] = fn
	return nil
}

// Unsubscribe unregisters an existing subscriber listening to channel.
func (a *Client) Unsubscribe(channel string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.subscribers[channel]; !ok {
		return fmt.Errorf("channel not listened to")
	}
	err := a.sub.QueueUnbind(a.queue, channel, a.exchange, nil)
	if err != nil {
		return fmt.Errorf("failed to unbind queue %q from exchange %q: %v", a.queue, a.exchange, err)
	}
	delete(a.subscribers, channel)
	return nil
}

// Publish sends a message to a channel.
func (a *Client) Publish(channel string, data interface{}, priority uint8) error {
	body, err := msgpack.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal to msgpack: %v", err)
	}
	msg := amqp.Publishing{Body: body,
		Priority:     priority,
		DeliveryMode: amqp.Persistent,
		Expiration:   strconv.Itoa(int(defaultTTL.Milliseconds())),
		Type:         pubSubMessageType}
	err = a.pub.PublishToExchange(&msg, a.exchange, channel, true)
	if err != nil {
		return fmt.Errorf("failed to publish to exchange %q channel %q: %v", a.exchange, channel, err)
	}
	return nil
}

func (a *Client) consume(ctx context.Context, reconnectAttemptDelay time.Duration, prefetchCount int) error {
	_, err := a.sub.Consume(ctx, a.queue, rabbitmq.ConsumeOpts{
		PrefetchCount:         prefetchCount,
		ReconnectAttemptCount: -1,
		ReconnectAttemptDelay: reconnectAttemptDelay,
	}, func(msg *amqp.Delivery) bool {
		a.mu.RLock()
		defer a.mu.RUnlock()
		if _, ok := a.subscribers[msg.RoutingKey]; !ok {
			if msg.ReplyTo == a.clientID {
				a.logger.Error("pub/sub message was not routed",
					log.String("headers", fmt.Sprintf("%v", msg.Headers)),
					log.String("body", fmt.Sprintf("%v", msg.Body)))
				return true
			}

			a.logger.Info("unsubscribing from unknown channel", log.String("channel", msg.RoutingKey))
			err := a.sub.QueueUnbind(a.queue, msg.RoutingKey, a.exchange, nil)
			if err != nil {
				a.logger.Error("failed to unsubscribe from channel", log.String("channel", msg.RoutingKey), log.Error(err))
			}
			return true
		}
		consumer := a.subscribers[msg.RoutingKey]

		var unpacked interface{}
		err := msgpack.Unmarshal(msg.Body, &unpacked)
		if err != nil {
			a.logger.Error("failed to unmarshal from msgpack", log.String("body", fmt.Sprintf("%v", msg.Body)), log.Error(err))
		}
		consumer(unpacked, msg.Redelivered)
		return true
	})
	return err
}
