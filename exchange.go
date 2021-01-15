package rabbitmq

import "github.com/streadway/amqp"

// ExchangeKind is exchange kind
type ExchangeKind string

// Exchange kinds supported by RabbitMQ
const (
	ExchangeDirect  ExchangeKind = "direct"
	ExchangeFanout  ExchangeKind = "fanout"
	ExchangeTopic   ExchangeKind = "topic"
	ExchangeHeaders ExchangeKind = "headers"
)

// ExchangeDeclare is a wrapper around amqp.ExchangeDeclare that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) ExchangeDeclare(name string, kind ExchangeKind,
	durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		return ch.channel.ExchangeDeclare(name, string(kind), durable, autoDelete, internal, noWait, args)
	})
}

// ExchangeDeclarePassive is a wrapper around amqp.ExchangeDeclarePassive that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) ExchangeDeclarePassive(name string, kind ExchangeKind,
	durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		return ch.channel.ExchangeDeclarePassive(name, string(kind), durable, autoDelete, internal, noWait, args)
	})
}

// ExchangeBind is a wrapper around amqp.ExchangeBind that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) ExchangeBind(destination, key, source string, noWait bool, args amqp.Table) error {
	return s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		return ch.channel.ExchangeBind(destination, key, source, noWait, args)
	})
}

// ExchangeDelete is a wrapper around amqp.ExchangeDelete that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		return ch.channel.ExchangeDelete(name, ifUnused, noWait)
	})
}

// ExchangeUnbind is a wrapper around amqp.ExchangeDelete that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) ExchangeUnbind(destination, key, source string, noWait bool, args amqp.Table) error {
	return s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		return ch.channel.ExchangeUnbind(destination, key, source, noWait, args)
	})
}
