package anycast

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/atomic"

	logf "git.acronis.com/abc/go-libs/log"
	"git.acronis.com/abc/go-libs/rabbitmq"

	"github.com/stretchr/testify/require"
)

var (
	globalPub     *rabbitmq.Publisher
	rabbitAddress string
	rabbitVhost   string
	user          string
	pass          string
	adminUser     string
	adminPass     string
	k8sNamespace  string
	ipv4s         map[string]struct{}
	logger        logf.FieldLogger
)

func amqpURL() *url.URL {
	u, _ := url.Parse(fmt.Sprintf("amqp://%v:%v@%v/%v", user, pass, rabbitAddress, url.PathEscape(rabbitVhost)))
	return u
}

func getIP4() map[string]struct{} {
	ip4s := make(map[string]struct{})
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip4s[ipnet.IP.String()] = struct{}{}
			}
		}
	}
	return ip4s
}

func TestMain(m *testing.M) {
	flag.StringVar(&rabbitAddress, "rabbit-addr", "", "rabbitmq address")
	flag.StringVar(&rabbitVhost, "rabbit-vhost", "", "rabbitmq vhost")
	flag.StringVar(&user, "user", "admin", "username for amqp client")
	flag.StringVar(&pass, "pass", "admin", "password for amqp client")
	flag.StringVar(&adminUser, "admin-user", "admin", "username for management api")
	flag.StringVar(&adminPass, "admin-pass", "admin", "password for management api")
	flag.StringVar(&k8sNamespace, "ns", "default", "k8s namespace")
	flag.Parse()

	if rabbitAddress == "" {
		fmt.Println("rabbitmq address not set, skip the test suite")
		os.Exit(0)
	}

	ipv4s = getIP4()
	log.Println(amqpURL())
	log.Println(ipv4s)
	logger, _ = logf.NewLogger(&logf.Config{Output: logf.OutputStdout, Level: logf.LevelDebug})
	var err error
	for i := 0; i < 10; i++ {
		globalPub, err = rabbitmq.NewDefaultPublisher(amqpURL(), logger)
		if err != nil {
			log.Println("couldn't prepare publisher", err)
			log.Println("...try again in a second")
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		log.Fatalln("couldn't prepare publisher", err)
	}

	exitVal := m.Run()
	os.Exit(exitVal)
}

type mockDialer struct {
	sync.Mutex
	networkOff bool
	conns      []net.Conn
}

func (d *mockDialer) disableNetwork() {
	d.Lock()
	defer d.Unlock()
	for _, conn := range d.conns {
		_ = conn.Close()
	}
	d.networkOff = true
}

func (d *mockDialer) enableNetwork() {
	d.Lock()
	defer d.Unlock()
	d.networkOff = false
}

func (d *mockDialer) dial() func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		d.Lock()
		defer d.Unlock()
		if d.networkOff {
			return nil, fmt.Errorf("network off")
		}
		conn, err := amqp.DefaultDial(time.Second*30)(network, addr)
		if err == nil {
			d.conns = append(d.conns, conn)
		}
		return conn, err
	}
}

func TestSubThenPub(t *testing.T) {
	clientID := "TestSubThenPub"
	channel := clientID + "_channel"
	a, err := NewClient(clientID, amqpURL(), Config{}, false, false, logger)
	require.NoError(t, err)
	defer a.Close()
	sendMsg := "hello sub pub"
	received := make(chan interface{})
	err = a.Subscribe(channel, func(msg interface{}, _ bool) {
		received <- msg
	})
	require.NoError(t, err)
	err = a.Publish(channel, &sendMsg, DefaultPriority)
	require.NoError(t, err)
	receivedMsg := <-received
	require.IsType(t, sendMsg, receivedMsg)
	require.Equal(t, sendMsg, receivedMsg.(string))
}

func TestPubThenSub(t *testing.T) {
	clientID := "TestPubThenSub"
	channel := clientID + "_channel"
	a, err := NewClient(clientID, amqpURL(), Config{}, false, false, logger)
	require.NoError(t, err)
	defer a.Close()
	sendMsg := "hello pub sub"
	err = a.Publish(channel, &sendMsg, DefaultPriority)
	require.NoError(t, err)
	received := make(chan interface{})
	err = a.Subscribe(channel, func(msg interface{}, _ bool) {
		received <- msg
	})
	require.NoError(t, err)
	select {
	case <-received:
		require.Fail(t, "no message expected")
	case <-time.After(time.Second):
	}
}

func TestQueueExists(t *testing.T) {
	clientID := "TestQueueExists"
	a, err := NewClient(clientID, amqpURL(), Config{}, false, false, logger)
	require.NoError(t, err)
	a.Close()

	_, err = globalPub.QueueInspect("pubsub." + clientID)
	require.NoError(t, err)
}

func TestQueueAutoDeleted(t *testing.T) {
	clientID := "TestQueueAutoDeleted"
	a, err := NewClient(clientID, amqpURL(), Config{}, false, true, logger)
	require.NoError(t, err)
	_, err = globalPub.QueueInspect("pubsub." + clientID)
	require.NoError(t, err)
	a.Close()
	_, err = globalPub.QueueInspect("pubsub." + clientID)
	require.Error(t, err)
}

func TestNoNetworkOnStart(t *testing.T) {
	clientID := "TestNoNetworkOnStart"
	networkOff := atomic.NewBool(true)
	dial := func(network, addr string) (net.Conn, error) {
		if networkOff.Load() {
			return nil, fmt.Errorf("network off")
		}
		return amqp.DefaultDial(time.Second*30)(network, addr)
	}
	a, err := NewClient(clientID, amqpURL(), Config{
		Publisher: rabbitmq.PublisherConfig{
			ConnectionConfig: rabbitmq.ConnectionConfig{Dial: dial},
		},
		Consumer: rabbitmq.ConsumerConfig{
			ConnectionConfig: rabbitmq.ConnectionConfig{Dial: dial},
		},
	}, false, false, logger)
	require.Error(t, err)
	require.Nil(t, a)
}

func TestNoNetworkOnPublishing(t *testing.T) {
	clientID := "TestNoNetworkOnPublishing"
	channel := clientID + "_channel"
	dialer := mockDialer{}
	a, err := NewClient(clientID, amqpURL(), Config{
		Publisher: rabbitmq.PublisherConfig{
			ConnectionConfig: rabbitmq.ConnectionConfig{Dial: dialer.dial()},
		},
		Consumer: rabbitmq.ConsumerConfig{
			ConnectionConfig: rabbitmq.ConnectionConfig{Dial: dialer.dial()},
		},
	}, false, false, logger)
	require.NoError(t, err)
	defer a.Close()

	pub := make(chan bool, 3)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		i := 0
		err = fmt.Errorf("")
		for {
			select {
			case <-ctx.Done():
				close(pub)
				return
			default:
			}
			newerr := a.Publish(channel, i, DefaultPriority)
			if (newerr == nil) != (err == nil) {
				pub <- newerr != nil
				err = newerr
			}
			i++
		}
	}()
	time.Sleep(time.Second * 5)
	dialer.disableNetwork()
	time.Sleep(time.Second * 5)
	dialer.enableNetwork()
	time.Sleep(time.Second * 5)
	cancel()
	var states []bool
	for b := range pub {
		states = append(states, b)
	}
	require.Equal(t, []bool{false, true, false}, states)
}

func TestNoNetworkOnConsume(t *testing.T) {
	clientID := "TestNoNetworkOnConsume"
	channel := clientID + "_channel"
	dialer := mockDialer{}
	a, err := NewClient(clientID, amqpURL(), Config{
		PrefetchCount: 1000,
		RetryDelay:    time.Second,
		Consumer: rabbitmq.ConsumerConfig{
			ConnectionConfig: rabbitmq.ConnectionConfig{
				Dial: dialer.dial(),
			},
		},
	}, false, false, logger)
	require.NoError(t, err)
	_, err = a.pub.QueuePurge("pubsub." + clientID)
	require.NoError(t, err)
	var sent atomic.Int64
	var received atomic.Int64
	err = a.Subscribe(channel, func(msg interface{}, redelivery bool) {
		received.Inc()
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_ = a.Publish(channel, sent.Load(), DefaultPriority)
			sent.Inc()
		}
	}()
	time.Sleep(time.Second)
	dialer.disableNetwork()
	time.Sleep(time.Second)
	dialer.enableNetwork()
	cancel()
	time.Sleep(time.Second * 5)
	a.Close()
	require.LessOrEqual(t, sent.Load(), received.Load()) // received may be larger due to redeliveries
}
