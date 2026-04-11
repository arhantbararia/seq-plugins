package worker

import (
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TaskHandler defines the function signature for processing a message.
// Implementations must Ack/Nack the delivery.
type TaskHandler func(d amqp.Delivery) 
// Consumer manages the RabbitMQ connection and consumption of tasks.
type Consumer struct {
	mu          sync.Mutex
	conn        *amqp.Connection
	url         string
	queueName   string
	consumerTag string
	handler     TaskHandler


	done chan struct{} // Signals shutdown
	wg   sync.WaitGroup
}



// NewConsumer creates a new Consumer.
func NewConsumer(url, queueName, consumerTag string, handler TaskHandler) *Consumer {
	return &Consumer{
		url:         url,
		queueName:   queueName,
		consumerTag: consumerTag,
		handler:     handler,
		done:        make(chan struct{}),
	}
}

// Start begins the consumer loop in a separate goroutine.
// It handles connections and reconnections.
func (c *Consumer) Start() {
	log.Printf("[Consumer] Starting for queue '%s'", c.queueName)
	c.wg.Add(1)
	go c.run()
}

// Stop gracefully shuts down the consumer.
func (c *Consumer) Stop() {
	c.mu.Lock()
	select {
	case <-c.done:
		// Already stopping.
	default:
		close(c.done)
	}
	if c.conn != nil && !c.conn.IsClosed() {
		// This will unblock the `for range deliveries` in connectAndConsume
		c.conn.Close()
	}
	c.mu.Unlock()

	log.Println("[Consumer] Waiting for run loop to finish...")
	c.wg.Wait()
	log.Println("[Consumer] Consumer stopped.")
}

// run is the main loop that connects and consumes.
func (c *Consumer) run() {
	defer c.wg.Done()

	for {
		select {
		case <-c.done:
			log.Println("[Consumer] Shutdown signal received, exiting run loop.")
			return
		default:
		}

		err := c.connectAndConsume()
		if err != nil {
			log.Printf("[Consumer] Error: %v. Retrying in 5 seconds...", err)
		}

		// Wait before retrying, unless we are shutting down.
		select {
		case <-c.done:
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// connectAndConsume establishes a connection and starts the message consumption loop.
func (c *Consumer) connectAndConsume() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	defer conn.Close()

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	log.Printf("[Consumer] Connected to RabbitMQ, queue '%s'", c.queueName)

	_, err = ch.QueueDeclare(c.queueName, true, false, false, false, nil)
	if err != nil {
		return err
	}

	deliveries, err := ch.Consume(c.queueName, c.consumerTag, false, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Printf("[Consumer] Waiting for messages on queue '%s'...", c.queueName)

	for d := range deliveries {
		c.handler(d)
	}

	// The loop exited, meaning the connection was closed.
	// Check if it was a graceful shutdown.
	select {
	case <-c.done:
		return nil // No error on graceful shutdown
	default:
		return fmt.Errorf("connection or channel closed unexpectedly")
	}
}
