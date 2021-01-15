package rabbitmq

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestConsumer_SingleChannel_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)
	require.NoError(t, globalPub.PublishToQueue(p("hi"), existingQueue, false))
	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{NoAck: true}, once(expectDelivery(t, "hi")))
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestConsumer_Canceled_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	count, err := cons.Consume(ctx, existingQueue, ConsumeOpts{NoAck: true}, expectDelivery(t, ""))
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestConsumer_Multiplex_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()

	count := defaultChannelPoolSize * 2
	for q := 0; q < count; q++ {
		name := fmt.Sprintf("test-consumer-%v", q)
		_, err = globalPub.QueueDeclare(name, false, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi from %v", name)), name, false))
	}
	var wg sync.WaitGroup
	wg.Add(count)
	for q := 0; q < count; q++ {
		go func(q int) {
			defer wg.Done()
			name := fmt.Sprintf("test-consumer-%v", q)
			got, consumeErr := cons.Consume(
				context.Background(), name, ConsumeOpts{NoAck: true}, once(expectDelivery(t, fmt.Sprintf("hi from %v", name))))
			require.NoError(t, consumeErr)
			require.Equal(t, 1, got)
		}(q)
	}
	wg.Wait()

	conns, err := getActiveConnections(rabbitVhost)
	require.NoError(t, err)
	require.Len(t, conns, 1)
}

func TestConnection_IsConnectedOnline(t *testing.T) {
	var notify = make(chan NotifyEvent)
	config := ConsumerConfig{ConnectionConfig{
		Notify: notify,
	}}
	cons, err := NewConfiguredConsumer(amqpURL(), config, logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	disconnectCount := 5
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer func() {
			cancel()
			wg.Done()
		}()
		for i := 0; i < disconnectCount; i++ {
			time.Sleep(time.Second)
			killConnection(t, rabbitVhost)
		}
	}()
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			require.Equal(t, true, cons.IsConnected())
			if ctx.Err() != nil {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
	}(ctx)
	var opened atomic.Int32
	go func() {
		for ev := range notify {
			if ev == OpenedConnection {
				opened.Inc()
			}
		}
	}()
	wg.Wait()
	require.Equal(t, disconnectCount, int(opened.Load()))
}

func TestConnection_IsConnectedOffline(t *testing.T) {
	var notify = make(chan NotifyEvent)
	var networkOff atomic.Bool
	config := ConsumerConfig{ConnectionConfig{
		Dial: func(network, addr string) (conn net.Conn, e error) {
			if networkOff.Load() {
				return nil, fmt.Errorf("mocked network off")
			}
			return amqp.DefaultDial(time.Second)(network, addr)
		},
		Notify: notify,
	}}
	cons, err := NewConfiguredConsumer(amqpURL(), config, logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	killConnection(t, rabbitVhost)
	networkOff.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		for {
			require.Equal(t, false, cons.IsConnected())
			if ctx.Err() != nil {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}
	}(ctx)
	var opened atomic.Int32
	go func() {
		for ev := range notify {
			if ev == OpenedConnection {
				opened.Inc()
			}
		}
	}()
	time.Sleep(time.Second * 5)
	cancel()
	require.Equal(t, 0, int(opened.Load()))
}

func TestConsumer_ConnectionClosedNoInterrupt_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			killConnection(t, rabbitVhost)
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("reconnect %d", i)), existingQueue, false))
		}
	}()
	fn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			require.Equal(t, fmt.Sprintf("reconnect %v", i), string(d.Body))
			i++
			return true
		}
	}
	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{ReconnectAttemptCount: 5, NoAck: true}, ntimes(5, fn(0)))
	require.NoError(t, err)
	require.Equal(t, 5, count)
	require.Equal(t, 1, len(cons.pool.channels))

	wg.Wait()
}

func TestConsumer_VhostDeleted_DeclareTransient_ConsumeQueue(t *testing.T) {
	const vhost = "TestConsumer_ConnectionClosed_DeclareTransient_ConsumeQueue"
	const queueName = "non-durable"
	redeclared := make(chan struct{})
	openConn := func(ch *amqp.Channel) error {
		logger.Debug("redeclare queue")
		_, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
		if err == nil {
			select {
			case redeclared <- struct{}{}:
			default:
			}
		}
		return err
	}
	u, _ := url.Parse(fmt.Sprintf("amqp://%v:%v@%v/%v", user, pass, rabbitAddress, vhost))
	mustCreateVhost(t, vhost, user)
	defer mustDeleteVhost(t, vhost)
	pub, err := NewConfiguredPublisher(u, PublisherConfig{
		Confirm: true,
	}, logger)
	require.NoError(t, err)
	cfg := ConsumerConfig{ConnectionConfig{OpenConn: openConn}}
	cons, err := NewConfiguredConsumer(u, cfg, logger)
	require.NoError(t, err)
	defer cons.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	const reconnectCount = 5
	var msgSent atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer func() {
			cancel()
			wg.Done()
		}()
		time.Sleep(time.Second) // let consumer start consuming first
		for i := 0; i < reconnectCount; i++ {
			go mustResetVhost(t, vhost, user)
			<-redeclared
			msg := fmt.Sprintf("reconnect %d", i)
			if err = pub.PublishToQueue(p(msg), queueName, true); err == nil {
				logger.Debugf("-> %s", msg)
				msgSent.Inc()
			} else {
				t.Logf("publish to queue failed: %v", err)
			}
			time.Sleep(time.Second)
		}
	}()
	fn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			logger.Debugf("<- %s", string(d.Body))
			i++
			return true
		}
	}
	count, err := cons.Consume(ctx, queueName,
		ConsumeOpts{ReconnectAttemptCount: 100, NoAck: true},
		fn(0))
	require.NoError(t, err)
	require.Equal(t, int(msgSent.Load()), count)

	wg.Wait()
}

func TestConsumer_RabbitRestarts_DeclareTransient_ConsumeQueue(t *testing.T) {
	const kubectl = "/usr/local/bin/kubectl"
	_, err := os.Stat(kubectl)
	if err != nil {
		t.Skipf("kubectl not found: %v", err)
	}
	defer func() {
		require.NoError(t, createGlobalPublisher()) // need to recreate global publisher after rabbit restarts
	}()
	const queueName = "non-durable"
	redeclared := make(chan struct{})
	openConn := func(ch *amqp.Channel) error {
		logger.Debug("redeclare queue")
		_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
		if err == nil {
			select {
			case redeclared <- struct{}{}:
			default:
			}
		}
		return err
	}
	pub, err := NewConfiguredPublisher(amqpURL(), PublisherConfig{
		Confirm: true,
	}, logger)
	require.NoError(t, err)
	defer pub.Close()
	cfg := ConsumerConfig{ConnectionConfig{OpenConn: openConn}}
	cons, err := NewConfiguredConsumer(amqpURL(), cfg, logger)
	require.NoError(t, err)
	defer cons.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	const reconnectCount = 5
	var msgSent atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer func() {
			cancel()
			wg.Done()
		}()
		time.Sleep(time.Second) // let consumer start consuming first
		for i := 0; i < reconnectCount; i++ {
			go restartRabbit(t, kubectl, k8sNamespace)
			<-redeclared
			msg := fmt.Sprintf("reconnect %d", i)
			if err = pub.PublishToQueue(p(msg), queueName, true); err == nil {
				logger.Debugf("-> %s", msg)
				msgSent.Inc()
			} else {
				t.Logf("publish to queue failed: %v", err)
			}
			time.Sleep(time.Second)
		}
	}()
	fn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			logger.Debugf("<- %s", string(d.Body))
			i++
			return true
		}
	}
	count, err := cons.Consume(ctx, queueName,
		ConsumeOpts{ReconnectAttemptCount: 100, NoAck: true},
		fn(0))
	require.NoError(t, err)
	require.Equal(t, int(msgSent.Load()), count)

	wg.Wait()
}

func TestConsumer_ConnectionClosedAutoAck_ConsumeQueue(t *testing.T) {
	networkOff := atomic.Bool{}
	dial := func(network, addr string) (net.Conn, error) {
		if networkOff.Load() {
			return nil, fmt.Errorf("network off")
		}
		return amqp.DefaultDial(30*time.Second)(network, addr)
	}

	cons, err := NewConfiguredConsumer(amqpURL(),
		ConsumerConfig{
			ConnectionConfig: ConnectionConfig{Dial: dial},
		},
		logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second)
		killConnection(t, rabbitVhost)
		time.Sleep(time.Second)
		err := globalPub.PublishToQueue(p("reconnect ok"), existingQueue, false) //nolint
		require.NoError(t, err)
		networkOff.Store(true)
		killConnection(t, rabbitVhost)
		time.Sleep(time.Second)
		err = globalPub.PublishToQueue(p("reconnect nok"), existingQueue, false) //nolint
		require.NoError(t, err)
	}()

	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{ReconnectAttemptCount: 3, NoAck: true},
		expectDelivery(t, "reconnect ok"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "network off")
	require.Equal(t, 1, count)

	wg.Wait()
}

func TestConsumer_DontInterfere_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()

	manualQueue := "manual-ack-queue"
	autoQueue := "auto-ack-queue"
	_, err = globalPub.QueueDeclare(manualQueue, false, false, false, false, nil)
	require.NoError(t, err)
	_, err = globalPub.QueueDeclare(autoQueue, false, false, false, false, nil)
	require.NoError(t, err)
	_, err = cons.QueuePurge(manualQueue)
	require.NoError(t, err)
	_, err = cons.QueuePurge(autoQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 100)
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi %v", i)), manualQueue, false))
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi %v", i)), autoQueue, false))
			if i == 5 {
				killConnection(t, rabbitVhost)
			}
		}
	}()

	fn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			require.Equal(t, fmt.Sprintf("hi %v", i), string(d.Body))
			i++
			return true
		}
	}

	manGot := 0
	go func() {
		defer wg.Done()
		manGot, err = cons.Consume(context.Background(), manualQueue, ConsumeOpts{}, fn(0)) //nolint
		require.Error(t, err)
		require.Contains(t, err.Error(), "channel closed and reconnect disabled")
		require.Less(t, manGot, 10)
	}()

	go func() {
		defer wg.Done()
		autoGot, err := cons.Consume(context.Background(), autoQueue, ConsumeOpts{ReconnectAttemptCount: -1, NoAck: true}, ntimes(10, fn(0))) //nolint
		require.NoError(t, err)
		require.Equal(t, 10, autoGot)
	}()
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		manLeft := 10 - manGot
		manGot, err = cons.Consume(context.Background(), manualQueue, ConsumeOpts{}, ntimes(manLeft, fn(manGot)))
		require.NoError(t, err)
		require.Equal(t, manLeft, manGot)
	}()
	wg.Wait()
}

func TestConsumer_PendingAcksOnReconnect_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()

	slowQueue := "slow-queue" //nolint
	fastQueue := "fast-queue" //nolint
	_, err = globalPub.QueueDeclare(slowQueue, false, false, false, false, nil)
	require.NoError(t, err)
	_, err = globalPub.QueueDeclare(fastQueue, false, false, false, false, nil)
	require.NoError(t, err)
	_, err = cons.QueuePurge(slowQueue)
	require.NoError(t, err)
	_, err = cons.QueuePurge(fastQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			time.Sleep(time.Millisecond * 100)
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi %v", i)), slowQueue, false))
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi %v", i)), fastQueue, false))
			if i == 5 {
				killConnection(t, rabbitVhost)
			}
		}
	}()

	fastfn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			require.Equal(t, fmt.Sprintf("hi %v", i), string(d.Body))
			i++
			return true
		}
	}
	slowfn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			require.Equal(t, fmt.Sprintf("hi %v", i), string(d.Body))
			i++
			time.Sleep(time.Second)
			return true
		}
	}

	go func() {
		defer wg.Done()
		_, err := cons.Consume(context.Background(), fastQueue, ConsumeOpts{ReconnectAttemptCount: -1}, fastfn(0)) //nolint
		require.Error(t, err)
		require.Contains(t, err.Error(), "channel closed while pending ack")
	}()

	go func() {
		defer wg.Done()
		_, err := cons.Consume(context.Background(), slowQueue, ConsumeOpts{ReconnectAttemptCount: -1}, slowfn(0)) //nolint
		require.Error(t, err)
		require.Contains(t, err.Error(), "channel closed while pending ack")
	}()

	wg.Wait()
}

func TestConsumer_PendingAcksOnRecycle_ConsumeQueue(t *testing.T) {
	cons, err := NewConfiguredConsumer(amqpURL(), ConsumerConfig{ConnectionConfig{
		RecyclePeriod: time.Second,
	}}, logger)
	require.NoError(t, err)
	defer cons.Close()

	slowQueue := "slow-queue" //nolint
	fastQueue := "fast-queue" //nolint
	_, err = globalPub.QueueDeclare(slowQueue, false, false, false, false, nil)
	require.NoError(t, err)
	_, err = globalPub.QueueDeclare(fastQueue, false, false, false, false, nil)
	require.NoError(t, err)
	_, err = cons.QueuePurge(slowQueue)
	require.NoError(t, err)
	_, err = cons.QueuePurge(fastQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(3)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer func() {
			cancel()
			wg.Done()
		}()
		for i := 0; i < 10; i++ {
			time.Sleep(time.Second)
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi %v", i)), slowQueue, false))
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi %v", i)), fastQueue, false))
		}
		time.Sleep(time.Second)
	}()

	fastfn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			require.Equal(t, fmt.Sprintf("hi %v", i), string(d.Body))
			i++
			return true
		}
	}
	slowfn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			require.Equal(t, fmt.Sprintf("hi %v", i), string(d.Body))
			i++
			time.Sleep(time.Second)
			return true
		}
	}

	go func() {
		defer wg.Done()
		got, err := cons.Consume(ctx, fastQueue, ConsumeOpts{ReconnectAttemptCount: -1}, fastfn(0)) //nolint
		assert.NoError(t, err)
		assert.Equal(t, 10, got)
	}()

	go func() {
		defer wg.Done()
		got, err := cons.Consume(ctx, slowQueue, ConsumeOpts{ReconnectAttemptCount: -1}, slowfn(0)) //nolint
		assert.NoError(t, err)
		assert.Equal(t, 10, got)
	}()

	wg.Wait()
}

func TestConsumer_ConnectionClosedPendingAck_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, globalPub.PublishToQueue(p("reconnect ok"), existingQueue, false))
		killConnection(t, rabbitVhost)
	}()

	fn := func(d *amqp.Delivery) bool {
		require.Equal(t, "reconnect ok", string(d.Body))
		time.Sleep(time.Second * 5)
		return true
	}
	_, err = cons.Consume(context.Background(), existingQueue, ConsumeOpts{}, fn)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pending ack")

	got, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{}, once(expectRedelivery(t, "reconnect ok")))
	require.NoError(t, err)
	require.Equal(t, 1, got)
	wg.Wait()
}

func TestConsumer_ClosedReceive_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second)
		cons.Close()
	}()

	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{NoAck: true}, expectDelivery(t, ""))
	require.Error(t, err)
	require.Equal(t, 0, count)
	wg.Wait()
}

func TestConsumer_NackRequeue_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, globalPub.PublishToQueue(p("hi nack"), existingQueue, false))
	}()

	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{},
		noAck(ntimes(3, ignoreDelivery(t, "hi nack"))))
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, 3)
	wg.Wait()
}

func TestConsumer_Ack_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, globalPub.PublishToQueue(p("hi ack"), existingQueue, false))
	}()

	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{}, once(expectDelivery(t, "hi ack")))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	wg.Wait()
}

func TestConsumer_AckReconnect_ConsumeQueue(t *testing.T) {
	cons, err := NewDefaultConsumer(amqpURL(), logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, globalPub.PublishToQueue(p("hi ack"), existingQueue, false))
		killConnection(t, rabbitVhost)
	}()

	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{}, expectDelivery(t, "hi ack"))
	require.Error(t, err)
	require.Equal(t, 1, count)
	wg.Wait()
}

func TestConsumer_RecycleConnection_ConsumeQueue(t *testing.T) {
	cons, err := NewConfiguredConsumer(amqpURL(),
		ConsumerConfig{ConnectionConfig: ConnectionConfig{RecyclePeriod: time.Second * 3}},
		logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 5)
		require.NoError(t, globalPub.PublishToQueue(p("hi recycle"), existingQueue, false))
	}()

	count, err := cons.Consume(context.Background(), existingQueue, ConsumeOpts{ReconnectAttemptCount: -1, NoAck: true},
		once(expectDelivery(t, "hi recycle")))
	require.NoError(t, err)
	require.Equal(t, 1, count)
	wg.Wait()
}

func TestConsumer_NotifyEvents_ConsumeQueue(t *testing.T) {
	notify := make(chan NotifyEvent, 100)
	events := make(map[NotifyEvent]int)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range notify {
			events[e]++
		}
	}()

	cons, err := NewConfiguredConsumer(amqpURL(),
		ConsumerConfig{
			ConnectionConfig: ConnectionConfig{
				RecyclePeriod: time.Second * 3,
				Notify:        notify,
			},
		},
		logger)
	require.NoError(t, err)
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	secs := 10
	msgPerSec := 100

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer func() {
			cancel()
			wg.Done()
		}()
		for i := 0; i < secs*msgPerSec; i++ {
			require.NoError(t, globalPub.PublishToQueue(p(fmt.Sprintf("hi %v", i)), existingQueue, false))
			time.Sleep(time.Second / time.Duration(msgPerSec))
		}
		time.Sleep(time.Millisecond * 100)
	}()
	fn := func(i int) DeliveryFunc {
		return func(d *amqp.Delivery) bool {
			require.Equal(t, fmt.Sprintf("hi %v", i), string(d.Body))
			i++
			return true
		}
	}
	count, err := cons.Consume(ctx, existingQueue, ConsumeOpts{ReconnectAttemptCount: -1, NoAck: true},
		ntimes(secs*msgPerSec, fn(0)))
	require.NoError(t, err)
	cons.Close()
	require.Equal(t, secs*msgPerSec, count)
	wg.Wait()
	require.Equal(t, secs/3+1, events[ClosedConnection])
	require.Equal(t, 1, events[ClosedChannel]) // channels closed with their parent connections
	require.Equal(t, secs/3, events[RecycledConnection])
	require.Equal(t, secs/3+1, events[OpenedConnection])
	require.Equal(t, secs/3+1, events[OpenedChannel])
}

func TestConsumer_CancelDuringReconnect_ConsumeQueue(t *testing.T) {
	networkOff := atomic.Bool{}
	dial := func(network, addr string) (net.Conn, error) {
		if networkOff.Load() {
			return nil, fmt.Errorf("network off")
		}
		return amqp.DefaultDial(30*time.Second)(network, addr)
	}

	cons, err := NewConfiguredConsumer(amqpURL(),
		ConsumerConfig{
			ConnectionConfig: ConnectionConfig{Dial: dial},
		},
		logger)
	require.NoError(t, err)
	defer cons.Close()
	_, err = cons.QueuePurge(existingQueue)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		networkOff.Store(true)
		killConnection(t, rabbitVhost)
	}()

	timeout := time.Duration(managementPluginResolution) * 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err = cons.Consume(ctx, existingQueue,
		ConsumeOpts{
			ReconnectAttemptCount: 3,
			ReconnectAttemptDelay: timeout,
			NoAck:                 true,
		},
		expectDelivery(t, "reconnect ok"))
	require.NoError(t, err)
}
