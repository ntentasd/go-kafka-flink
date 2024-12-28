package kafka

import (
	"log"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/ntentasd/kafka-flink/consumer/logger"
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
		correlationID := extractCorrelationID(message)

		logger.LogMessageConsumed(message.Topic, message.Partition, message.Offset, string(message.Value), correlationID)
		session.MarkMessage(message, "") // Mark message as processed.
	}
	return nil
}

func CreateConsumerGroup(brokers []string, config *sarama.Config, group string, maxRetries int, initialDelay time.Duration, maxDelay time.Duration) (sarama.ConsumerGroup, error) {
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

		// Check if retries are exhausted
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

func extractCorrelationID(message *sarama.ConsumerMessage) string {
	// Extract correlation ID from headers
	var correlationID string
	for _, header := range message.Headers {
		if string(header.Key) == "correlation-id" {
			correlationID = string(header.Value)
			break
		}
	}

	// Handle missing correlation ID
	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	return correlationID
}
