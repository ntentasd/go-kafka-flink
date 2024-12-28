package app

import (
	"net/http"

	"github.com/IBM/sarama"
)

type Application struct {
	Producer sarama.AsyncProducer
	Server   *http.ServeMux
	Port     string
}
