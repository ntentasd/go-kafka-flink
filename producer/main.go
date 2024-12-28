package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/ntentasd/kafka-flink/producer/app"
	"github.com/ntentasd/kafka-flink/producer/kafka"
	"github.com/ntentasd/kafka-flink/producer/logger"
)

func main() {
	bh := os.Getenv("BOOTSTRAP_HOST")
	bp := os.Getenv("BOOTSTRAP_PORT")
	bs := fmt.Sprintf("%s:%s", bh, bp)

	// configure sarama
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Ensure all brokers acknowledge the message.
	config.Producer.Retry.Max = 5                    // Retry up to 5 times if sending fails.
	config.Producer.Return.Successes = true          // Return success messages.
	config.Producer.Return.Errors = true             // Return error messages.

	// initialize producer
	maxRetries := 5
	initialDelay := 500 * time.Millisecond
	maxDelay := 10 * time.Second

	producer, err := kafka.CreateProducer([]string{bs}, config, maxRetries, initialDelay, maxDelay)
	if err != nil {
		log.Fatalf("Failed to create producer after %d retries: %v\n", maxRetries, err)
	}
	defer producer.Close()

	// consume from channels so it won't block
	go func() {
		for success := range producer.Successes() {
			message, err := encoderToString(success.Value)
			if err != nil {
				// handle this error
				continue
			}

			correlationID := extractCorrelationID(success)
			logger.LogMessageSent(success.Topic, success.Partition, success.Offset, message, correlationID)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Failed to produce message: %v\n", err.Err)
		}
	}()

	server := http.NewServeMux()

	app := app.Application{
		Producer: producer,
		Server:   server,
	}

	err = app.StartServer()
	if err != nil {
		log.Fatal(err)
	}
}

func encoderToString(encoder sarama.Encoder) (string, error) {
	bytes, err := encoder.Encode()
	if err != nil {
		return "", fmt.Errorf("failed to encode: %w", err)
	}

	return string(bytes), nil
}

func extractCorrelationID(message *sarama.ProducerMessage) string {
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
