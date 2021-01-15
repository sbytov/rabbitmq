package rabbitmq

import (
	"context"
	"net/url"
	"time"

	"github.com/streadway/amqp"

	"git.acronis.com/abc/go-libs/log"
)

// ConsumerConfig controls consumer parameters.
type ConsumerConfig struct {

	// ConnectionConfig defines parameters of underlying connection.
	ConnectionConfig
}

// DeliveryFunc is a callback function that is called by consumer for every delivered message.
// Return value true means that it must continue delivering; false means must cancel.
type DeliveryFunc func(*amqp.Delivery) bool

// Consumer represents AMQP connections used for consuming.
type Consumer struct {
	*session
}

// NewDefaultConsumer creates a new consumer with default parameters connected to given RabbitMQ server url.
// The url must be in form of amqp://user:password@address[:port]/vhost.
// If connection attempt fails an error will be returned.
//
// The consumer returned will internally keep a single connection and a channel pool so that
// it may multiplex more than one consume operation on its connection.
// The channel stays in use until corresponding Fetcher will be closed.
// If there are no free channels in the pool a consume attempt may block until some channel is freed.
// Default channel pool size is 10.
//
// The underlying connection will be periodically recycled (reconnected) to enhance load balancing and robustness.
// (see Acronis AMQP guidelines for more details).
// This reconnection must not happen if consumer is in manual acknowledgement mode and there are pending acknowledgements.
// Default recycle period is 30 minutes.
//
// If manual acknowledgement mode is going to be used on this consumer note the default prefetch count 1.
// This is good if you want to distribute the load between multiple consumers consuming in the round-robin fashion and
// if network latency is low.
// This value might become too low if you aim for a high throughput, refer to AMQP guidelines to find more details.
// It doesn't have any effect in auto-acknowledgement mode.
//
//nolint: interfacer //
func NewDefaultConsumer(rabbitURL *url.URL, logger log.FieldLogger) (*Consumer, error) {
	return NewConfiguredConsumer(rabbitURL, ConsumerConfig{}, logger)
}

// NewConfiguredConsumer creates a new consumer configured with cfg connected to given RabbitMQ server url.
// The url must be in form of amqp://user:password@address[:port]/vhost.
// If connection attempt fails an error will be returned.
//
// The consumer returned will internally keep a single connection and a channel pool so that
// it may multiplex more than one consume operation on its connection.
// The channel stays in use until corresponding Fetcher will be closed.
// If there are no free channels in the pool a consume attempt may block until some channel is freed.
// Channel pool size is controlled by cfg.ChannelPoolSize.
//
// The underlying connection will be periodically recycled (reconnected) to enhance load balancing and robustness.
// (see Acronis AMQP guidelines for more details).
// This reconnection must not happen if consumer is in manual acknowledgement mode and there are pending acknowledgements.
// Connection recycle period is controlled by cfg.RecyclePeriod (recommended value is 20-30 minutes).
//
// If manual acknowledgement mode is going to be used on this consumer consider what the correct cfg.Prefetch count
// should be. It doesn't have any effect in auto-acknowledgement mode.
//nolint: interfacer //
func NewConfiguredConsumer(rabbitURL *url.URL, cfg ConsumerConfig, logger log.FieldLogger) (*Consumer, error) {
	session, err := newSession(rabbitURL.String(), cfg.ConnectionConfig, false, logger)
	if err != nil {
		return nil, err
	}
	session.logger.Info("amqp consumer started", log.String("amqp_server_url", rabbitURL.String()))
	return &Consumer{session: session}, nil
}

// ConsumeOpts are options for consume.
type ConsumeOpts struct {
	NoAck     bool
	Exclusive bool
	NoWait    bool
	Args      amqp.Table

	// ReconnectAttemptCount if it is >= 0 limits number of reconnect attempts before returning an error.
	// If it is set to -1 consumer will try to reconnect indefinitely.
	ReconnectAttemptCount int

	// ReconnectAttemptDelay sets an interval before next reconnect attempt.
	// If not set a default value of 1s will be used.
	ReconnectAttemptDelay time.Duration

	// PrefetchCount defines how many unacknowledged messages can be kept in the library buffer or be inflight.
	// This setting only makes sense if manual acknowledge mode is used.
	// A too small prefetch count may hurt performance since RabbitMQ is waiting a lot to be able to send more messages.
	// If you have many consumers and a short processing time a lower prefetch value is recommended.
	// A too low value will keep the consumers idle since they need to wait for messages to arrive.
	// A too high value may keep one consumer busy, while other consumers are being kept in an idling state.
	// If you have many consumers and/or a long processing time, we recommend you to set prefetch count to 1
	// so that messages are evenly distributed among all your workers.
	PrefetchCount int
}

// Consume immediately starts delivering queued messages and blocks until channel is closed or consume canceled.
// For every delivered message the consumer will call provided callback function fn that is supposed to do delivery
// processing.
// It is an application code responsibility to make sure this callback doesn't panic.
//
// There are two ways to cancel consume either by signaling on the ctx passed in or returning false from
// the callback function. After cancel has been initiated there might be still pending messages inflight or in library
// buffer so the application must expect subsequent calls of callback function.
//
// Consume will return a count of messages delivered to fn and an error if underlying channel/connection was closed.
//
// Consume tries to take a channel from the channel pool (or create a new one if pool capacity allows).
// If there are no free channels it may block until some channel used by another consume operation is freed.
//nolint:gocyclo
func (cons *Consumer) Consume(ctx context.Context, queue string, opts ConsumeOpts, fn DeliveryFunc) (int, error) {
	if opts.ReconnectAttemptDelay == 0 {
		opts.ReconnectAttemptDelay = defaultReconnectDelay
	}
	if opts.PrefetchCount == 0 {
		opts.PrefetchCount = defaultPrefetchCount
	}
	f, err := cons.consume(queue, opts)
	defer func() {
		cons.free(f)
	}()
	if err != nil {
		return 0, err
	}
	cancel := false
	for {
		open := false
		recycle := false
		var d amqp.Delivery
		select {
		case <-f.cb.expired:
			recycle = true
		case d, open = <-f.in:
		case <-ctx.Done():
			cancel = true
		}

		if !cancel && !recycle {
			if open {
				cancel, err = cons.dispatchDelivery(f, &d, fn)
				if err != nil {
					return f.messageCount, err
				}
			} else {
				// delivery chan is closed
				// attempt to reconnect after connection loss
				mc := f.messageCount
				f, err = cons.reconnect(ctx, f, false)
				if f == nil {
					return mc, err
				}
			}
		}

		if cancel || recycle { // don't else it - cancel might have changed within upper if block
			// the reason we cancelling is either fn returned false, or context done signaled
			// or maybe we want to recycle which means we cancel and then reconnect instead of exiting
			cancel, err = cons.cancelDrain(f, fn, recycle)
			if err != nil {
				return f.messageCount, err
			}
			if recycle {
				// attempt to reconnect on recycle
				mc := f.messageCount
				f, err = cons.reconnect(ctx, f, true)
				if f == nil {
					return mc, err
				}
				continue
			}
			// closed due to the cancellation, this is ok
			return f.messageCount, nil
		}
	}
}

const (
	defaultPrefetchCount   int = 1
	defaultReconnectDelay      = time.Second
	pendingAckWaitInterval     = time.Millisecond * 100
)

type acknowledger struct {
	called bool
	target amqp.Acknowledger
}

func (a *acknowledger) Ack(tag uint64, multiple bool) error {
	a.called = true
	return a.target.Ack(tag, multiple)
}

func (a *acknowledger) Nack(tag uint64, multiple, requeue bool) error {
	a.called = true
	return a.target.Nack(tag, multiple, requeue)
}

func (a *acknowledger) Reject(tag uint64, requeue bool) error {
	a.called = true
	return a.target.Reject(tag, requeue)
}

func (cons *Consumer) dispatchDelivery(f *fetcher, d *amqp.Delivery, fn DeliveryFunc) (cancel bool, err error) {
	a := acknowledger{
		called: false,
		target: d.Acknowledger,
	}
	d.Acknowledger = &a
	if !f.params.NoAck {
		f.cb.pendingAcks.Inc()
		defer func() {
			if !a.called {
				err = a.Ack(d.DeliveryTag, false)
				if err != nil {
					err = &AMQPError{"channel closed while pending ack", err, f.ch.id}
				}
			}
			f.cb.pendingAcks.Dec()
		}()
	}
	f.messageCount++
	cancel = !fn(d)
	return
}

// can return (nil, nil) if context signals done
func (cons *Consumer) reconnect(ctx context.Context, f *fetcher, expired bool) (*fetcher, error) {
	err := checkPendingAcks(f, expired)
	if err != nil {
		return nil, err
	}
	cons.connmu.Lock()
	if cons.cb == f.cb { // make sure another channel haven't reestablished connection already
		cons.invalid.Store(true)
	}
	cons.connmu.Unlock()
	cons.free(f) // return previously used channel to the pool
	if f.params.ReconnectAttemptCount == 0 {
		return nil, &AMQPError{"channel closed and reconnect disabled", nil, f.ch.id}
	}
	for i := 0; i < f.params.ReconnectAttemptCount || f.params.ReconnectAttemptCount < 0; i++ {
		var newf *fetcher
		newf, err = cons.consume(f.queue, f.params)
		if err != nil {
			cons.logger.Info("reconnect failed", log.Int("attempt", i), log.Error(err))
			select {
			case <-ctx.Done():
				return nil, nil
			case <-time.After(f.params.ReconnectAttemptDelay):
			}
			continue
		}
		newf.messageCount = f.messageCount
		return newf, nil
	}
	return nil, &AMQPError{"channel closed and failed to reconnect", err, f.ch.id}
}

func checkPendingAcks(f *fetcher, expired bool) error {
	if f.params.NoAck {
		return nil
	}
	if expired {
		waitOtherFetchers(f)
	} else if f.cb.pendingAcks.Load() > 0 {
		return &AMQPError{"channel closed while pending ack", nil, f.ch.id}
	}
	return nil
}

func waitOtherFetchers(f *fetcher) {
	var waited bool
	var start = time.Now()
	for {
		activeFetchers := f.cb.activeFetchers.Load()
		if activeFetchers == 0 {
			if waited {
				took := time.Since(start)
				f.logger.Debugf("no pending fetchers, waited for %vms", int(took/time.Millisecond))
			} else {
				f.logger.Debug("no pending fetchers")
			}
			break
		}
		if !waited {
			f.logger.Debugf("waiting for pending %v fetchers", activeFetchers)
			waited = true
		}
		time.Sleep(pendingAckWaitInterval)
	}
}

func (cons *Consumer) consume(queue string, params ConsumeOpts) (*fetcher, error) {
	var deliveries <-chan amqp.Delivery
	var channel *lazyChannel
	var cb *connection
	var err error
	id := newUUID()
	err = cons.ensureChannelAndDo(false, func(ch *lazyChannel) error {
		channel = ch
		cb = ch.parent.cb
		deliveries, err = ch.channel.Consume(queue,
			id,
			params.NoAck,
			params.Exclusive,
			false,
			params.NoWait,
			params.Args,
		)
		if err != nil {
			return &AMQPError{"failed to consume", err, ch.id}
		}
		if !params.NoAck {
			err = ch.channel.Qos(params.PrefetchCount, 0, false)
			if err != nil {
				return &AMQPError{"failed to set qos", err, ch.id}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	logger := channel.logger.With(log.String("consumer_id", id))
	logger.Debug("consume started")
	cb.activeFetchers.Inc()
	return &fetcher{
		in:     deliveries,
		cb:     cb,
		ch:     channel,
		id:     id,
		queue:  queue,
		params: params,
		logger: logger}, nil
}

func (cons *Consumer) free(f *fetcher) {
	if f != nil {
		cons.pool.returnChannel(f.ch)
	}
}

func (cons *Consumer) cancelDrain(f *fetcher, fn DeliveryFunc, recycle bool) (cancel bool, err error) {
	err = f.ch.channel.Cancel(f.id, false)
	if err != nil {
		return false, err
	}
	f.logger.Debug("consume cancelling", log.Bool("recycle", recycle))
	for d := range f.in {
		d := d
		cancel, err = cons.dispatchDelivery(f, &d, fn)
		if err != nil {
			return cancel, err
		}
	}
	f.logger.Debug("consume canceled")
	f.cb.activeFetchers.Dec()
	return
}

type fetcher struct {
	in           <-chan amqp.Delivery
	cb           *connection
	ch           *lazyChannel
	id           string
	queue        string
	params       ConsumeOpts
	messageCount int
	logger       log.FieldLogger
}
