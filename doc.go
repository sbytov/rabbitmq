// Package rabbitmq provides RabbitMQ client following Acronis AMQP guidelines
// (https://adn.acronis.com/display/DCO/AMQP+guidelines).
// This client is a wrapper around https://github.com/streadway/amqp.
// Key points:
// - segregates publishing from consuming
// - enforces prefetch count for consumers
// - recycles connections periodically
// - auto-reconnect on channel/connection close
// - multiplexes multiple pubs/cons over a single connection
// - keeps channel pool to reduce channel re-openings
package rabbitmq
