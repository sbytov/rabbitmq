package rabbitmq

import "github.com/streadway/amqp"

// QueueDeclare is a wrapper around amqp.QueueDeclare that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	var queue amqp.Queue
	var err error
	err = s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		queue, err = ch.channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
		return err
	})
	return queue, err
}

// QueueDeclarePassive is a wrapper around amqp.QueueDeclarePassive that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool,
	args amqp.Table) (amqp.Queue, error) {
	var queue amqp.Queue
	var err error
	err = s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		queue, err = ch.channel.QueueDeclarePassive(name, durable, autoDelete, exclusive, noWait, args)
		return err
	})
	return queue, err
}

// QueuePurge is a wrapper around amqp.QueuePurge that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) QueuePurge(queue string) (int, error) {
	var purged int
	return purged, s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		var err error
		purged, err = ch.channel.QueuePurge(queue, false)
		return err
	})
}

// QueueDelete is a wrapper around amqp.QueueDelete that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	var purged int
	return purged, s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		var err error
		purged, err = ch.channel.QueueDelete(name, ifUnused, ifEmpty, noWait)
		return err
	})
}

// QueueBind is a wrapper around amqp.QueueBind that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		return ch.channel.QueueBind(name, key, exchange, noWait, args)
	})
}

// QueueUnbind is a wrapper around amqp.QueueUnbind that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) QueueUnbind(name, key, exchange string, args amqp.Table) error {
	return s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		return ch.channel.QueueUnbind(name, key, exchange, args)
	})
}

// QueueInspect is a wrapper around amqp.QueueInspect that (re-)establishes channel if needed.
// It requires a free channel available in the channel pool and may block if there is none available.
func (s *session) QueueInspect(name string) (amqp.Queue, error) {
	var queue amqp.Queue
	return queue, s.ensureChannelAndDo(true, func(ch *lazyChannel) error {
		var err error
		queue, err = ch.channel.QueueInspect(name)
		return err
	})
}
