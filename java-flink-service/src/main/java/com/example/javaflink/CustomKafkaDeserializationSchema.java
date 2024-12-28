package com.example.javaflink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public class CustomKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<ConsumerRecord<String, String>> {

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ConsumerRecord<String, String>> collector) {
    // Extract the key if it exists
    String key = record.key() != null ? new String(record.key()) : null;
    // Extract the value
    String value = record.value() != null ? new String(record.value()) : null;

    // Extract the "correlation-id" from the headers
    String correlationId = null;
    if (record.headers() != null) {
      for (Header header : record.headers()) {
        if ("correlation-id".equalsIgnoreCase(header.key())) {
          correlationId = new String(header.value());
        }
      }
    }

    // Create a new ConsumerRecord with the correct key, value
    ConsumerRecord<String, String> processedRecord = new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            key,
            value
    );

    // Add correlation-id to the headers for further propagation
    if (correlationId != null) {
      processedRecord.headers().add("correlation-id", correlationId.getBytes());
    }

    // Collect the correctly processed record
    collector.collect(processedRecord);
  }

  @Override
  public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
    return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>() {});
  }
}