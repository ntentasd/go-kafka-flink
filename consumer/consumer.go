package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	// setup actions - not needed for now
	return nil
}

func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	// setup actions - not needed for now
	return nil
}

func (ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Consume messages from the topic/partition
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "") // Mark message as processed.
	}
	return nil
}

func main() {
	bh := os.Getenv("BOOTSTRAP_HOST")
	bp := os.Getenv("BOOTSTRAP_PORT")
	bs := fmt.Sprintf("%s:%s", bh, bp)

	// 1. Configure Sarama
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Retry parameters
	maxRetries := 5
	initialDelay := 500 * time.Millisecond
	maxDelay := 10 * time.Second

	// 2. Create Consumer Group
	group, err := createConsumerGroup([]string{bs}, config, "go-group", maxRetries, initialDelay, maxDelay)
	if err != nil {
		log.Fatalf("Failed to create consumer group after %d retries: %v\n", maxRetries, err)
	}
	defer group.Close()

	// 3. Consume Messages
	ctx := context.Background()
	handler := ConsumerGroupHandler{}
	for {
		if err := group.Consume(ctx, []string{"output-topic"}, handler); err != nil {
			log.Printf("Error consuming messages: %v\n", err)
		}
	}
}

func createConsumerGroup(brokers []string, config *sarama.Config, group string, maxRetries int, initialDelay time.Duration, maxDelay time.Duration) (sarama.ConsumerGroup, error) {
	var consumerGroup sarama.ConsumerGroup
	var err error

	// Initialize backoff retry
	delay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		consumerGroup, err = sarama.NewConsumerGroup(brokers, group, config)
		if err == nil {
			return consumerGroup, nil
		}

		log.Printf("Attempt %d: Failed to create consumer group: %v\n", attempt, err)

		// Check if we've exhausted retries
		if attempt == maxRetries {
			break
		}

		// Apply jitter to delay
		jitter := time.Duration(rand.Int63n(int64(delay) / 2))
		time.Sleep(delay + jitter)

		// Exponential backoff
		delay = delay * 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return nil, err
}
