package rabbitmq

import (
	"fmt"
	"net"
	"sync"
	"time"

	shortuuid "github.com/lithammer/shortuuid/v3"
	"github.com/streadway/amqp"
	"go.uber.org/atomic"

	"git.acronis.com/abc/go-libs/log"
)

// OpenConnFunc is a callback function that is called every time connection is reopen.
type OpenConnFunc func(*amqp.Channel) error

// ConnectionConfig controls connection parameters.
type ConnectionConfig struct {

	// ChannelPoolSize defines the size of channel pool owned by the connection.
	// Any Publisher or Consumer operation would attempt to take a channel from this pool so keep it reasonably large.
	ChannelPoolSize int

	// RecyclePeriod defines how often the connection will be recycled i.e. reconnected.
	// This helps to enhance load balancing and robustness and is part of Acronis AMQP guidelines.
	// Recommended value is 20-30 minutes.
	RecyclePeriod time.Duration

	// Dial returns a net.Conn prepared for a TLS handshake with TSLClientConfig,
	// then an AMQP connection handshake.
	// If Dial is nil, net.DialTimeout with a 30s connection and 30s deadline is
	// used during TLS and AMQP handshaking.
	Dial func(network, addr string) (net.Conn, error)

	// Heartbeat value defines after what period of time the connection should be considered unreachable (down) by RabbitMQ.
	Heartbeat time.Duration

	// CloseNotify is a channel that can be used by application to be notified about connection/channek close events etc.
	// For instance this can be useful to collect metrics.
	Notify chan NotifyEvent

	// OpenConn is a callback that if not nil is called every time connection is reopen.
	// The purpose of this callback is to declare necessary topology in case it is transient (non-durable) to deal with
	// RabbitMQ server restarts.
	// It is an application code responsibility to make sure this callback doesn't panic.
	// Return value indicates an error during topology declaration.
	OpenConn OpenConnFunc
}

// NotifyEvent is a connection/channel lifecycle event.
type NotifyEvent int

func (e NotifyEvent) String() string {
	switch e {
	case ClosedConnection:
		return "closed connection"
	case ClosedChannel:
		return "closed channel"
	case RecycledConnection:
		return "recycled connection"
	case OpenedChannel:
		return "opened channel"
	case OpenedConnection:
		return "opened connection"
	default:
		return "unknown"
	}
}

// Connection/channel lifecycle events.
const (
	ClosedConnection NotifyEvent = iota
	ClosedChannel
	RecycledConnection
	OpenedConnection
	OpenedChannel
)

// AMQPError is an error type that is returned as a result of an AMQP call.
type AMQPError struct {
	Message string
	Inner   error
	Channel string
}

func (e *AMQPError) Error() string {
	str := fmt.Sprintf("%v", e.Message)
	if e.Inner != nil {
		str += fmt.Sprintf(": %v", e.Inner)
	}
	if e.Channel != "" {
		str += fmt.Sprintf(" (amqp_channel_id=%v)", e.Channel)
	}
	return str
}

// connection keeps resources that are reallocated for every reopened amqp connection
type connection struct {
	pendingAcks    atomic.Uint64 // reference counter to keep track on pending acks to figure out if reconnect is possible
	activeFetchers atomic.Uint64 // reference counter to keep track on active consume ops to figure out if recycle is possible
	expired        chan struct{} // gets closed after recycleTicker ticks to broadcast the expiration event
	recycleTicker  *time.Ticker
	connection     *amqp.Connection
	logger         log.FieldLogger
}

// channelPool is a pool of channels that are reused by session
type channelPool struct {
	mu           sync.Mutex
	channels     []*lazyChannel
	channelSlots chan struct{}
	freeChannels chan *lazyChannel
}

func makePool(size int) *channelPool {
	cp := channelPool{
		channels:     nil,
		channelSlots: make(chan struct{}, size),
		freeChannels: make(chan *lazyChannel, size),
	}
	for i := 0; i < size; i++ {
		cp.channelSlots <- struct{}{}
	}
	return &cp
}

func (cp *channelPool) dispose() {
	for _, ch := range cp.channels {
		ch.dispose()
	}
}

func (cp *channelPool) invalidate() {
	for _, ch := range cp.channels {
		ch.invalid.Store(true)
	}
}

func (cp *channelPool) takeChannel(s *session) *lazyChannel {
	<-cp.channelSlots // take an available slot to make sure we don't exceed pool size
	var ch *lazyChannel
	select {
	case ch = <-cp.freeChannels:
		// took a free channel from the pool
	default:
		// no free channels in the pool, make one
		channelID := newUUID()
		ch = &lazyChannel{parent: s, id: channelID, logger: s.logger.With(log.String("amqp_channel_id", channelID))}
		cp.mu.Lock()
		cp.channels = append(cp.channels, ch)
		cp.mu.Unlock()
	}
	return ch
}

func (cp *channelPool) returnChannel(ch *lazyChannel) {
	cp.freeChannels <- ch         // return the channel to the pool
	cp.channelSlots <- struct{}{} // announce the slot is available
}

// session is an abstraction over amqp connection
// every publisher or consumer IS a session
// the goal is to make connection breaks/reconnects transparent to the client code
// contains a channel pool
type session struct {
	url     string
	invalid atomic.Bool // this flags indicates if underlying connection needs to be reestablished
	confirm bool        // TODO: this belongs to publisher
	closed  bool
	wg      sync.WaitGroup // keeps track on spawned goroutines to make sure they don't leak

	pool *channelPool

	logger log.FieldLogger
	cfg    ConnectionConfig

	mu sync.RWMutex // this mutes synchronizes between Close and other public API calls

	connmu sync.Mutex  // this mutex protects connection
	cb     *connection // for each reopened connection it will point to a new instance
}

// IsConnected returns true if underlying connection to RabbitMQ is alive.
func (s *session) IsConnected() bool {
	err := s.ensureChannelAndDo(true, nil)
	if err != nil {
		s.logger.Info("session is disconnected", log.Error(err))
		return false
	}
	return true
}

// Close releases underlying connection and channels and stops internal goroutines.
func (s *session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true

	s.pool.dispose()
	if s.cb != nil {
		err := s.cb.close()
		if err != nil {
			s.logger.Warn("failed to close connection", log.Error(err))
		}
	}
	s.wg.Wait() // wait for goroutines listening to close/return chan's to finish
	if s.cfg.Notify != nil {
		close(s.cfg.Notify)
	}
}

func (s *session) isOpen() bool {
	return s.cb != nil && !s.invalid.Load()
}

func (s *session) open() (*connection, error) {
	conn, err := amqp.DialConfig(s.url, amqp.Config{
		Heartbeat: s.cfg.Heartbeat,
		Locale:    "en_US",
		Dial:      s.cfg.Dial,
	})
	if err != nil {
		return nil, &AMQPError{"failed to dial server", err, ""}
	}
	closeRcv := make(chan *amqp.Error)
	conn.NotifyClose(closeRcv)

	cb := &connection{
		expired:       make(chan struct{}),
		recycleTicker: time.NewTicker(s.cfg.RecyclePeriod),
		connection:    conn,
		logger:        s.logger.With(log.String("amqp_connection_id", newUUID())),
	}
	s.wg.Add(1)
	go func() {
		for {
			select {
			case <-cb.recycleTicker.C:
				if !cb.isExpired() {
					s.logger.Info("connection expired")
					close(cb.expired)
				}
			case e := <-closeRcv:
				if e != nil {
					s.logger.Warn("connection closed by server", log.Error(e))
				}
				s.invalid.Store(true)
				s.notify(ClosedConnection, cb.logger)
				s.wg.Done()
				return
			}
		}
	}()
	return cb, nil
}

func newUUID() string {
	return shortuuid.New()
}

func (cb *connection) close() error {
	cb.recycleTicker.Stop()
	if cb.connection.IsClosed() {
		return nil
	}
	return cb.connection.Close()
}

func (cb *connection) isExpired() bool {
	select {
	case <-cb.expired:
		return true
	default:
		return false
	}
}

func (s *session) ensureOpen() (*amqp.Channel, error) {
	var err error
	if s.cb != nil {
		err = s.cb.close()
		if err != nil {
			s.logger.Warn("failed to close connection", log.Error(err))
		}
		s.cb = nil
	}
	s.pool.invalidate()
	s.cb, err = s.open()
	if err != nil {
		return nil, &AMQPError{"failed to open connection", err, ""}
	}
	ch, err := s.cb.connection.Channel()
	if err != nil {
		return nil, &AMQPError{"failed to open channel", err, ""}
	}
	if s.cfg.OpenConn != nil {
		err = s.cfg.OpenConn(ch)
		if err != nil {
			_ = ch.Close()
			return nil, &AMQPError{"failed on openconn callback", err, ""}
		}
	}
	s.invalid.Store(false)
	s.notify(OpenedConnection, s.cb.logger)
	return ch, nil
}

func (s *session) ensureConnection() (*amqp.Channel, error) {
	s.connmu.Lock()
	defer s.connmu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("this connection is closed")
	}
	if s.cb != nil &&
		s.cb.isExpired() &&
		s.cb.activeFetchers.Load() == 0 {
		s.invalid.Store(true)
		s.notify(RecycledConnection, s.cb.logger)
	}
	if !s.isOpen() {
		return s.ensureOpen()
	}
	return nil, nil
}

func (s *session) ensureChannelAndDo(autoFree bool, fn func(ch *lazyChannel) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	newChan, err := s.ensureConnection()
	if err != nil {
		return err
	}
	ch := s.pool.takeChannel(s)
	defer func() {
		if autoFree {
			s.pool.returnChannel(ch)
		}
	}()
	if !ch.isOpen() {
		if newChan != nil {
			err = ch.assign(newChan, s.cb)
		} else {
			err = ch.open(s.cb)
		}
		if err != nil {
			return err
		}
	}
	if fn != nil {
		return fn(ch)
	}
	return nil
}

func (s *session) notify(event NotifyEvent, logger log.FieldLogger) {
	logger.Debug(event.String())
	select {
	case s.cfg.Notify <- event:
	default:
	}
}

const (
	defaultChannelPoolSize int = 10
	defaultRecyclePeriod       = 30 * time.Minute
	defaultHeartbeat           = 10 * time.Second
)

func newSession(url string, cfg ConnectionConfig, confirm bool, logger log.FieldLogger) (*session, error) {
	if cfg.ChannelPoolSize == 0 {
		cfg.ChannelPoolSize = defaultChannelPoolSize
	}
	if cfg.RecyclePeriod == 0 {
		cfg.RecyclePeriod = defaultRecyclePeriod
	}
	if cfg.Heartbeat == 0 {
		cfg.Heartbeat = defaultHeartbeat
	}
	con := &session{
		url:     url,
		confirm: confirm,
		logger:  logger.With(log.String("amqp_session_id", newUUID())),
		cfg:     cfg,
	}
	con.pool = makePool(cfg.ChannelPoolSize)
	err := con.ensureChannelAndDo(true, nil)
	if err != nil {
		return nil, err
	}
	return con, nil
}

type lazyChannel struct {
	channel    *amqp.Channel
	invalid    atomic.Bool
	parent     *session
	confirmRcv chan amqp.Confirmation
	returnRcv  chan amqp.Return
	id         string
	logger     log.FieldLogger
}

func (ch *lazyChannel) open(conn *connection) error {
	var err error
	// channel has been closed automatically when its parent connection was closed
	// it is safe to call ch.channel.Close again but would result in an error 'channel/connection is not open' so just skip
	newChan, err := conn.connection.Channel()
	if err != nil {
		return &AMQPError{"failed to open channel", err, ch.id}
	}
	return ch.assign(newChan, conn)
}

func (ch *lazyChannel) assign(channel *amqp.Channel, conn *connection) error {
	ch.logger = conn.logger.With(log.String("amqp_channel_id", ch.id))
	ch.channel = channel
	ch.parent.notify(OpenedChannel, ch.logger)
	ch.invalid.Store(false)
	closeRcv := make(chan *amqp.Error)
	ch.confirmRcv = make(chan amqp.Confirmation)
	ch.returnRcv = make(chan amqp.Return)
	ch.channel.NotifyClose(closeRcv)
	ch.parent.wg.Add(1)
	go func() {
		for e := range closeRcv {
			if e != nil {
				ch.parent.logger.Warn("channel closed by server", log.Error(e))
				ch.parent.invalid.Store(true)
			} else {
				ch.invalid.Store(true)
			}
			ch.parent.notify(ClosedChannel, ch.logger)
		}
		ch.parent.wg.Done()
	}()
	if ch.parent.confirm {
		err := ch.channel.Confirm(false)
		if err != nil {
			return &AMQPError{"failed to put in confirm mode", err, ch.id}
		}
		ch.channel.NotifyPublish(ch.confirmRcv)
		ch.channel.NotifyReturn(ch.returnRcv)
	}
	return nil
}

func (ch *lazyChannel) isOpen() bool {
	return ch.channel != nil && !ch.invalid.Load()
}

func (ch *lazyChannel) dispose() {
	if ch.isOpen() {
		err := ch.channel.Close()
		if err != nil {
			ch.parent.logger.Warn("failed to close channel", log.Error(err))
			close(ch.returnRcv)
			close(ch.confirmRcv)
		} else {
			ch.parent.notify(ClosedChannel, ch.logger)
		}
	}
}
