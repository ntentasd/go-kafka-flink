package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/ntentasd/kafka-flink/consumer/kafka"
)

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
	group, err := kafka.CreateConsumerGroup([]string{bs}, config, "go-group", maxRetries, initialDelay, maxDelay)
	if err != nil {
		log.Fatalf("Failed to create consumer group after %d retries: %v\n", maxRetries, err)
	}
	defer group.Close()

	// 3. Consume Messages
	ctx := context.Background()
	handler := kafka.ConsumerGroupHandler{}
	for {
		if err := group.Consume(ctx, []string{"output-topic"}, handler); err != nil {
			log.Printf("Error consuming messages: %v\n", err)
		}
	}
}
