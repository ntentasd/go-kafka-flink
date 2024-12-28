package com.example.javaflink;

public class KafkaMessage {
    private final String key;
    private final String value;
    private final String correlationId;

    public KafkaMessage(String key, String value, String correlationId) {
        this.key = key;
        this.value = value;
        this.correlationId = correlationId;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", correlationId='" + correlationId + '\'' +
                '}';
    }
}
