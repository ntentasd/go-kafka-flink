package com.example.javaflink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {
  public static void main(String[] args) throws Exception {
    // Set up the execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(3);

    // Kafka Consumer
    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
      .setBootstrapServers("kafka:9092") // Kafka server
      .setTopics("input-topic") // Kafka topic
      .setGroupId("flink-group") // Consumer group ID
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize messages as strings
      .build();

    System.out.println("Starting Flink Kafka consumer...");

    KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
      .setBootstrapServers("kafka:9092") // Kafka server
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("output-topic") 
          .setValueSerializationSchema(new SimpleStringSchema()) // Serialize messages as strings
          .build()
      )
      .build();

      // Add Kafka source to Flink, process the messages, and send to Kafka sink
      env.fromSource(kafkaSource, org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(), "Kafka Source")
        .map(value -> {
          System.out.println("Received message: " + value);
          return value.toUpperCase();
        }) // Log and transform
        .sinkTo(kafkaSink); // Write to Kafka sink

      // Execute the Flink job
      env.execute("Kafka Flink Job");
  }
}
