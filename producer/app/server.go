package app

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/ntentasd/kafka-flink/producer/logger"
	"github.com/ntentasd/kafka-flink/producer/models"
)

func (app *Application) StartServer() error {
	app.Server.HandleFunc("/events", app.addEvent)

	logger.LogStart()
	err := http.ListenAndServe(":8000", app.Server)
	if err != nil {
		return err
	}

	return nil
}
func (app *Application) addEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Header().Set("Content-Type", "application/json")
		return
	}

	var req models.EventRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json")
		return
	}
	defer r.Body.Close()

	// generate a unique correlation ID
	correlationID := uuid.New().String()

	// create the kafka message
	message := &sarama.ProducerMessage{
		Topic: "input-topic",
		Value: sarama.StringEncoder(req.Message),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("correlation-id"),
				Value: []byte(correlationID),
			},
		},
	}

	select {
	case app.Producer.Input() <- message:
	default:
		log.Println("Producer input channel is full, dropping message")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{\"error\":\"producer overloaded\"}"))
		return
	}

	res := models.EventResponse{
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
