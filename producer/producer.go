package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type Application struct {
	Producer sarama.AsyncProducer
	Server   *http.ServeMux
	Port     string
}

func createProducer(brokers []string, config *sarama.Config, maxRetries int, initialDelay time.Duration, maxDelay time.Duration) (sarama.AsyncProducer, error) {
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

func main() {
	bh := os.Getenv("BOOTSTRAP_HOST")
	bp := os.Getenv("BOOTSTRAP_PORT")
	bs := fmt.Sprintf("%s:%s", bh, bp)

	// 1. Configure Sarama
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Ensure all brokers acknowledge the message.
	config.Producer.Retry.Max = 5                    // Retry up to 5 times if sending fails.
	config.Producer.Return.Successes = true          // Return success messages.
	config.Producer.Return.Errors = true             // Return error messages.

	// 2. Initialize Producer
	maxRetries := 5
	initialDelay := 500 * time.Millisecond
	maxDelay := 10 * time.Second

	producer, err := createProducer([]string{bs}, config, maxRetries, initialDelay, maxDelay)
	if err != nil {
		log.Fatalf("Failed to create producer after %d retries: %v\n", maxRetries, err)
	}
	defer producer.Close()

	go func() {
		for success := range producer.Successes() {
			log.Printf("Message sent successfully to topic %s, partition %d, offset %d\n", success.Topic, success.Partition, success.Offset)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Failed to produce message: %v\n", err.Err)
		}
	}()

	server := http.NewServeMux()

	app := Application{
		Producer: producer,
		Server:   server,
	}

	err = app.StartServer()
	if err != nil {
		log.Fatal(err)
	}
}

func (app *Application) StartServer() error {
	app.Server.HandleFunc("/events", app.addEvent)

	log.Println("Server listening on port 8000...")
	err := http.ListenAndServe(":8000", app.Server)
	if err != nil {
		return err
	}

	return nil
}

type EventRequest struct {
	Message string `json:"message"`
}

type EventResponse struct {
	Message string `json:"message"`
	Topic   string `json:"topic"`
}

func (app *Application) addEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Header().Set("Content-Type", "application/json")
		return
	}

	var req EventRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		return
	}
	defer r.Body.Close()

	message := &sarama.ProducerMessage{
		Topic: "input-topic",
		Value: sarama.StringEncoder(req.Message),
	}

	select {
	case app.Producer.Input() <- message:
		log.Printf("Message: %s successfully sent to kafka topic\n", req.Message)
	default:
		log.Println("Producer input channel is full, dropping message")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"error\":\"producer overloaded\"}"))
		return
	}

	res := EventResponse{
		Message: "event successfully ingested",
		Topic:   "input-topic",
	}

	responseData, err := json.Marshal(res)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	w.Write(responseData)
}
