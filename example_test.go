package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

//nolint: gocritic
func ExampleConsumer_Consume() {
	url := amqpURL() // RabbitMQ url, should look like amqp://guest:guest@myrabbitmq/myvhost
	pub, err := NewDefaultPublisher(url, logger)
	if err != nil {
		log.Fatal("couldn't create publisher")
	}
	defer pub.Close()
	const queue = "solar-system"
	_, err = pub.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		log.Fatal("couldn't create queue")
	}
	for _, planet := range []string{"mercury", "venus", "earth", "mars", "jupiter"} {
		err = pub.PublishToQueue(p(planet), queue, false)
		if err != nil {
			log.Fatal("failed to publish to queue")
		}
	}

	cons, err := NewDefaultConsumer(url, logger)
	if err != nil {
		log.Fatal("couldn't create consumer")
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	_, err = cons.Consume(ctx, queue, ConsumeOpts{}, func(planet *amqp.Delivery) bool {
		fmt.Println(string(planet.Body))
		return true
	})
	if err != nil {
		log.Fatal("failed to consume from queue")
	}
	// Output:
	// mercury
	// venus
	// earth
	// mars
	// jupiter
}
