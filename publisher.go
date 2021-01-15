package rabbitmq

import (
	"fmt"
	"net/url"

	"github.com/streadway/amqp"

	"git.acronis.com/abc/go-libs/log"
)

// PublisherConfig controls publisher parameters.
type PublisherConfig struct {

	// ConnectionConfig defines parameters of underlying connection.
	ConnectionConfig

	// Confirm requires the server to confirms messages as it handles them by sending an acknowledgement.
	// If publisher is in confirm mode it will wait until acknowledgement received before returning from publish.
	Confirm bool
}

// Publisher represents AMQP connection used for publishing.
type Publisher struct {
	*session
}

// PublishToExchange publishes pub to specific exchange with the given routeKey.
// Publish tries to take a channel from the channel pool (or create a new one if pool capacity allows).
// If there are no free channels it may block until some channel used by another publish operation is freed.
func (p *Publisher) PublishToExchange(pub *amqp.Publishing, exchange, routeKey string, mandatory bool) error {
	return p.publish(pub, mandatory, exchange, routeKey)
}

// PublishToQueue publishes pub directly to specific queue using default exchange and queue as a route key
// Publish tries to take a channel from the channel pool (or create a new one if pool capacity allows).
// If there are no free channels it may block until some channel used by another publish operation is freed.
func (p *Publisher) PublishToQueue(pub *amqp.Publishing, queue string, mandatory bool) error {
	return p.publish(pub, mandatory, "", queue)
}

// NewDefaultPublisher creates a new publisher with default parameters connected to given RabbitMQ server url.
// The url must be in form of amqp://user:password@address[:port]/vhost.
// If connection attempt fails an error will be returned.
//
// The publisher returned will internally keep a single connection and a channel pool so that
// it may multiplex more than one publishing on its connection.
// If there are no free channels in the pool a publish attempt may block until some channel is freed.
// Default channel pool size is 10.
//
// The underlying connection will be periodically recycled (reconnected) to enhance load balancing and robustness.
// (see Acronis AMQP guidelines for more details).
// This reconnection must be transparent to application.
// Default recycle period is 30 minutes.
//
// Default publisher doesn't set confirm mode.
//nolint: interfacer
func NewDefaultPublisher(rabbitURL *url.URL, logger log.FieldLogger) (*Publisher, error) {
	return NewConfiguredPublisher(rabbitURL, PublisherConfig{}, logger)
}

// NewConfiguredPublisher creates a new publisher configured with cfg and connected to given RabbitMQ server url.
// The url must be in form of amqp://user:password@address[:port]/vhost.
// If connection attempt fails an error will be returned.
//
// The publisher returned will internally keep a single connection and a channel pool so that
// it may multiplex more than one publishing on its connection.
// If there are no free channels in the pool a publish attempt may block until some channel is freed.
// Channel pool size is controlled by cfg.ChannelPoolSize.
//
// The underlying connection will be periodically recycled (reconnected) to enhance load balancing and robustness.
// (see Acronis AMQP guidelines for more details).
// This reconnection must be transparent to application.
// Connection recycle period is controlled by cfg.RecyclePeriod (recommended value is 20-30 minutes).
//
// If you want to ensure that publishings have successfully been received by the server you can enable confirm mode
// by settings cfg.Confirm but it will make publish slower and give additional load on server and network.
//nolint: interfacer
func NewConfiguredPublisher(rabbitURL *url.URL, cfg PublisherConfig, logger log.FieldLogger) (*Publisher, error) {
	session, err := newSession(rabbitURL.String(), cfg.ConnectionConfig, cfg.Confirm, logger)
	if err != nil {
		return nil, err
	}
	session.logger.Info("amqp publisher started", log.String("amqp_server_url", rabbitURL.String()))
	return &Publisher{session}, nil
}

// PubReturnedError is an error returned when publishing gets returned to publisher.
type PubReturnedError struct {
	// Return contains details got from underlying library.
	Return amqp.Return
}

func (e *PubReturnedError) Error() string {
	return "publishing returned to publisher"
}

// PubNotAckedError is an error returned when server responded to publishing with negative acknowledgment.
type PubNotAckedError struct{}

func (e *PubNotAckedError) Error() string {
	return "server responded with nack"
}

func (p *Publisher) publish(pub *amqp.Publishing, mandatory bool, exchange, routeKey string) error {
	if pub == nil {
		return fmt.Errorf("publishing cannot be nil")
	}
	return p.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		err := ch.channel.Publish(exchange, routeKey, mandatory, false, *pub)
		if err != nil {
			return err
		}
		if p.confirm {
			for {
				select {
				case returned, ok := <-ch.returnRcv:
					if !ok {
						return fmt.Errorf("channel closed")
					}
					p.logger.Debug("returned to publisher",
						log.String("exch", returned.Exchange),
						log.String("key", returned.RoutingKey))
					err = &PubReturnedError{Return: returned}
				case confirmed, ok := <-ch.confirmRcv:
					if !ok {
						return fmt.Errorf("channel closed")
					}
					p.logger.Debug("confirmation received",
						log.Bool("ack", confirmed.Ack),
						log.Int64("tag", int64(confirmed.DeliveryTag)))
					if !confirmed.Ack && err == nil {
						return &PubNotAckedError{}
					}
					return err
				}
			}
		}
		return nil
	})
}
