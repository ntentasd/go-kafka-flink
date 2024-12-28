package com.example.javaflink;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.charset.StandardCharsets;

public class CustomKafkaSerializationSchema implements KafkaRecordSerializationSchema<KafkaMessage> {

  private final String topic;

  public CustomKafkaSerializationSchema(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(KafkaMessage message, KafkaSinkContext context, Long timestamp) {
    // Create headers
    RecordHeaders headers = new RecordHeaders();
    if (message.getCorrelationId() != null) {
      headers.add("correlation-id", message.getCorrelationId().getBytes(StandardCharsets.UTF_8));
    }

    // Construct the ProducerRecord with generics
    return new ProducerRecord<byte[], byte[]>(
            topic,
            null, // Partition (null means let Kafka decide)
            timestamp,
            message.getKey() != null ? message.getKey().getBytes(StandardCharsets.UTF_8) : null,
            message.getValue().getBytes(StandardCharsets.UTF_8),
            headers
    );
  }
}