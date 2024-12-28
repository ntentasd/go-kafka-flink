package com.example.javaflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

public class Job {
  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);

    // Kafka Consumer
    KafkaSource<ConsumerRecord<String, String>> kafkaSource = KafkaSource.<ConsumerRecord<String, String>>builder()
      .setBootstrapServers("kafka:9092") // Kafka server
       .setTopics("input-topic")
      .setGroupId("flink-group") // Consumer group ID
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(new CustomKafkaDeserializationSchema())
      .build();

    System.out.println("Starting Flink Kafka consumer...");

    KafkaSink<KafkaMessage> kafkaSink = KafkaSink.<KafkaMessage>builder()
      .setBootstrapServers("kafka:9092") // Kafka server
      .setRecordSerializer(new CustomKafkaSerializationSchema("output-topic"))
      .build();

      // Add Kafka source to Flink, process the messages, and send to Kafka sink
      env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
        .map(record -> {
          String key = record.key();
          String value = record.value();
          String correlationId = null;

          // Extract headers
          for (Header header : record.headers()) {
            if ("correlation-id".equalsIgnoreCase(header.key())) {
              correlationId = new String(header.value());
              break;
            }
          }

          System.out.printf("Received message: %s, Correlation ID: %s%n", value, correlationId);

          return new KafkaMessage(key, value.toUpperCase(), correlationId);
        })
        .sinkTo(kafkaSink); // Write to Kafka sink

      // Execute the Flink job
      env.execute("Kafka Flink Job");
  }
}
