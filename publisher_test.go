package rabbitmq

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"go.uber.org/atomic"

	"github.com/streadway/amqp"

	"github.com/stretchr/testify/require"
)

func TestPublisher_SingleChannel_PublishQueue(t *testing.T) {
	pub, err := NewDefaultPublisher(amqpURL(), logger)
	require.NoError(t, err)
	conns, err := getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)
	require.Equal(t, 0, conns[0].Channels)

	for i := 0; i < 10; i++ {
		err = pub.PublishToQueue(p("hello"), existingQueue, false)
		require.NoError(t, err)
	}

	conns, err = getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)

	pub.Close()

	conns, err = getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 0)
}

func TestPublisher_Multiplex_PublishQueue(t *testing.T) {
	pub, err := NewDefaultPublisher(amqpURL(), logger)
	require.NoError(t, err)
	conns, err := getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)
	require.Equal(t, 0, conns[0].Channels)

	for i := 0; i < 100; i++ {
		go func(i int) {
			publishErr := pub.PublishToQueue(p(fmt.Sprintf("hello%v", i)), existingQueue, false)
			require.NoError(t, publishErr)
		}(i)
	}

	conns, err = getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)

	pub.Close()

	conns, err = getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 0)
}

func TestPublisher_ConfirmOk_PublishQueue(t *testing.T) {
	pub, err := NewConfiguredPublisher(amqpURL(), PublisherConfig{Confirm: true}, logger)
	require.NoError(t, err)
	defer pub.Close()
	for i := 0; i < 10; i++ {
		err = pub.PublishToQueue(p("hello"), existingQueue, true)
		require.NoError(t, err)
	}
}

func TestPublisher_ConfirmNok_PublishQueue(t *testing.T) {
	pub, err := NewConfiguredPublisher(amqpURL(), PublisherConfig{Confirm: true}, logger)
	require.NoError(t, err)
	defer pub.Close()
	for i := 0; i < 10; i++ {
		err = pub.PublishToQueue(p("hello"), "non-existing-queue", true)
		require.Error(t, err, "publishing returned to publisher")
	}
}

func TestPublisher_ConnectionClosed_PublishQueue(t *testing.T) {
	pub, err := NewDefaultPublisher(amqpURL(), logger)
	require.NoError(t, err)
	defer pub.Close()

	conns, err := getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)
	require.Equal(t, 0, conns[0].Channels)

	err = pub.PublishToQueue(p("hello"), existingQueue, false)
	require.NoError(t, err)

	killConnection(t, rabbitVhost)

	err = pub.PublishToQueue(p("hello"), existingQueue, false)
	require.NoError(t, err)
	err = pub.PublishToQueue(p("hello"), existingQueue, false)
	require.NoError(t, err)
}

func TestPublisher_ConnectionRecycled_PublishQueue(t *testing.T) {
	pub, err := NewConfiguredPublisher(amqpURL(), PublisherConfig{ConnectionConfig: ConnectionConfig{RecyclePeriod: time.Second * 3}}, logger)
	require.NoError(t, err)
	defer pub.Close()
	conns, err := getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)
	require.Equal(t, 0, conns[0].Channels)

	err = pub.PublishToQueue(p("hello"), existingQueue, false)
	require.NoError(t, err)

	time.Sleep(time.Second * 5)

	err = pub.PublishToQueue(p("hello"), existingQueue, false)
	require.NoError(t, err)
}

func TestPublisher_RedeclareNonDurable_PublishQueue(t *testing.T) {
	const vhost = "TestPublisher_RedeclareNonDurable_PublishQueue"
	const queueName = "non-durable"
	openConn := func(ch *amqp.Channel) error {
		_, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
		return err
	}
	cfg := PublisherConfig{
		ConnectionConfig: ConnectionConfig{
			OpenConn: openConn,
		},
		Confirm: true,
	}
	mustCreateVhost(t, vhost, user)
	defer mustDeleteVhost(t, vhost)
	u, _ := url.Parse(fmt.Sprintf("amqp://%v:%v@%v/%v", user, pass, rabbitAddress, vhost))
	pub, err := NewConfiguredPublisher(u, cfg, logger)
	require.NoError(t, err)
	defer pub.Close()

	conns, err := getActiveConnections(vhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)
	require.Equal(t, 0, conns[0].Channels)

	err = pub.PublishToQueue(p("hello"), queueName, true)
	require.NoError(t, err)

	mustResetVhost(t, vhost, user)
	killAllConnections(t, vhost)

	err = pub.PublishToQueue(p("hello"), queueName, true)
	require.NoError(t, err)
}

func TestPublisher_FailRedeclareNonDurable_PublishQueue(t *testing.T) {
	const vhost = "TestPublisher_FailRedeclareNonDurable_PublishQueue"
	const queueName = "non-durable"
	var redeclare atomic.Bool
	openConn := func(ch *amqp.Channel) error {
		if redeclare.Load() {
			return fmt.Errorf("failed to redeclare queue")
		}
		_, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
		return err
	}
	cfg := PublisherConfig{
		ConnectionConfig: ConnectionConfig{
			OpenConn: openConn,
		},
		Confirm: true,
	}
	mustCreateVhost(t, vhost, user)
	defer mustDeleteVhost(t, vhost)
	u, _ := url.Parse(fmt.Sprintf("amqp://%v:%v@%v/%v", user, pass, rabbitAddress, vhost))
	pub, err := NewConfiguredPublisher(u, cfg, logger)
	require.NoError(t, err)
	defer pub.Close()

	conns, err := getActiveConnections(vhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)
	require.Equal(t, 0, conns[0].Channels)

	err = pub.PublishToQueue(p("hello"), queueName, true)
	require.NoError(t, err)

	redeclare.Store(true)
	mustResetVhost(t, vhost, user)
	killAllConnections(t, vhost)

	err = pub.PublishToQueue(p("hello"), queueName, true)
	require.EqualError(t, err, "failed on openconn callback: failed to redeclare queue")
}
