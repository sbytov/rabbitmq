package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	flags "github.com/jessevdk/go-flags"

	"git.acronis.com/abc/go-libs/log"
	"git.acronis.com/abc/go-libs/rabbitmq"
	"git.acronis.com/abc/go-libs/rabbitmq/anycast"
)

// Options is command line options.
type Options struct {
	ClientID   string   `short:"c" long:"client-id" description:"Unique client ID, i.e queue name"`
	Channel    []string `short:"C" long:"channel" description:"Channel name. Can be defined multiple times" required:"true"`
	RabbitURL  string   `short:"U" long:"amqp-url" description:"RabbitMQ connection URL" required:"true"`
	LogLevel   string   `short:"l" long:"log-level" description:"Logging level" default:"info" choice:"info" choice:"debug" choice:"warn" choice:"error"` //nolint
	Durable    bool     `long:"durable" description:"Declare queue durable"`
	AutoDelete bool     `long:"auto-delete" description:"Auto-delete queue"`
	Prefetch   int      `long:"prefetch-count" description:"Prefetch count (qos) for client" default:"10"`
}

type pubCommand struct {
	Format  string `long:"format" description:"Message format" default:"json" choice:"json" choice:"text" choice:"binary"` //nolint
	Data    string `short:"D" long:"data" description:"Message payload"`
	Confirm bool   `long:"confirm" description:"Require publishing confirmation"`
}

type subCommand struct {
}

var opts Options
var logger log.FieldLogger

// Execute pub sub command.
func (c *pubCommand) Execute([]string) error {
	a, err := newAnycastClient(c.Confirm)
	if err != nil {
		return err
	}
	defer a.Close()

	var obj interface{}
	for _, ch := range opts.Channel {
		switch c.Format {
		case "json":
			err = json.Unmarshal([]byte(c.Data), &obj)
			if err != nil {
				return fmt.Errorf("failed to unmarshal %q to json: %v", c.Data, err)
			}
		default:
			obj = c.Data
		}

		err = a.Publish(ch, obj, anycast.DefaultPriority)
		if err != nil {
			logger.Error("failed to publish message", log.String("channel", ch), log.String("message", c.Data), log.Error(err))
		}
		logger.Info("successfully published message", log.String("channel", ch), log.String("message", c.Data))
	}
	return nil
}

// Execute sub subcommand.
func (c *subCommand) Execute([]string) error {
	a, err := newAnycastClient(false)
	if err != nil {
		return err
	}
	defer a.Close()
	fmt.Println("========================\nListening to channels...\n- Ctrl+C to cancel\n========================")
	for _, ch := range opts.Channel {
		ch := ch
		err := a.Subscribe(ch, func(msg interface{}, redelivery bool) {
			var dst bytes.Buffer
			bytes, err := json.Marshal(msg)
			if err != nil {
				logger.Error("failed to marshal message", log.String("channel", ch), log.String("message", fmt.Sprintf("%v", msg)), log.Error(err))
			}
			logger.Debug("received message", log.String("channel", ch), log.String("message", string(bytes)), log.Bool("redelivery", redelivery))
			_ = json.Indent(&dst, bytes, "", "\t")
			fmt.Println(dst.String())
		})
		if err != nil {
			logger.Error("failed to subscribe to channel", log.String("channel", ch), log.Error(err))
		}
	}
	ctrlc := make(chan os.Signal, 1)
	signal.Notify(ctrlc, os.Interrupt, syscall.SIGTERM)
	<-ctrlc
	fmt.Println("========================\n- Ctrl+C pressed in Terminal\n========================")
	return nil
}

func newAnycastClient(confirm bool) (*anycast.Client, error) {
	u, err := url.Parse(opts.RabbitURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url %q: %v", opts.RabbitURL, err)
	}
	logger, _ = log.NewLogger(&log.Config{Output: log.OutputStdout, Format: log.FormatJSON, Level: log.Level(opts.LogLevel)})
	a, err := anycast.NewClient(opts.ClientID, u, anycast.Config{
		Publisher: rabbitmq.PublisherConfig{Confirm: confirm},
	}, opts.Durable, opts.AutoDelete, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create anycast client: %v", err)
	}
	return a, err
}

func main() {
	p := flags.NewParser(&opts, flags.Default)
	_, _ = p.AddCommand("pub", "Publish to PUB/SUB channel", "", &pubCommand{})
	_, _ = p.AddCommand("sub", "Subscribe to PUB/SUB channel", "", &subCommand{})
	_, err := p.Parse()
	if err != nil {
		fmt.Println("failed to parse arguments", err)
		p.WriteHelp(os.Stdout)
		os.Exit(3)
	}
}
