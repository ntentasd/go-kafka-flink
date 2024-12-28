package logger

import (
	"go.uber.org/zap"
)

var logger, _ = zap.NewProduction()

func LogMessageConsumed(topic string, partition int32, offset int64, message string, correlationID string) {
	logger.Info("Message consumed",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.String("message", message),
		zap.String("correlation_id", correlationID),
	)
}
