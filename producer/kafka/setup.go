package kafka

import (
	"log"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
)

func CreateProducer(brokers []string, config *sarama.Config, maxRetries int, initialDelay time.Duration, maxDelay time.Duration) (sarama.AsyncProducer, error) {
	var producer sarama.AsyncProducer
	var err error

	// Initialize backoff retry
	delay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		producer, err = sarama.NewAsyncProducer(brokers, config)
		if err == nil {
			return producer, nil
		}

		log.Printf("Attempt %d: Failed to create producer: %v\n", attempt, err)

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
