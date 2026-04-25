package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"rss_trigger/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
	url     string
}

func NewPublisher() *Publisher {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}
	p := &Publisher{url: url}
	if err := p.connect(); err != nil {
		log.Printf("[Publisher] Initial connection failed (will retry on publish): %v", err)
	}
	return p
}

func (p *Publisher) connect() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("channel: %w", err)
	}
	p.conn = conn
	p.channel = ch
	log.Println("[Publisher] Connected to RabbitMQ")
	return nil
}

func (p *Publisher) ensureConnected() error {
	if p.conn != nil && !p.conn.IsClosed() {
		return nil
	}
	log.Println("[Publisher] Reconnecting to RabbitMQ...")
	return p.connect()
}

// Publish sends a TriggerEvent to the EVENT_MESSAGE exchange
// with the workflow_id as the routing key.
func (p *Publisher) Publish(workflowID string, event models.TriggerEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureConnected(); err != nil {
		return fmt.Errorf("publisher not connected: %w", err)
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = p.channel.PublishWithContext(ctx,
		"EVENT_MESSAGE", // exchange (topic exchange declared by executor)
		workflowID,      // routing key = workflow_id
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		// Invalidate connection so next call reconnects
		p.conn = nil
		return fmt.Errorf("publish: %w", err)
	}

	log.Printf("[Publisher] Published event capability=%s workflow=%s", event.CapabilityKey, workflowID)
	return nil
}
