package models

type EventRequest struct {
	Message string `json:"message"`
}

type EventResponse struct {
	Message string `json:"message"`
	Topic   string `json:"topic"`
}
