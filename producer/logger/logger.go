package logger

import (
	"go.uber.org/zap"
)

var logger, _ = zap.NewProduction()

func LogMessageSent(topic string, partition int32, offset int64, message string, correlationID string) {
	logger.Info("Message sent",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.String("message", message),
		zap.String("correlation_id", correlationID),
	)
}

func LogStart() {
	logger.Info("Server listening on port :8000")
}
