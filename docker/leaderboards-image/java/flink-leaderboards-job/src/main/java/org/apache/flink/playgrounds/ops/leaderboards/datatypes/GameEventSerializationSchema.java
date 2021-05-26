package org.apache.flink.playgrounds.ops.leaderboards.datatypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * A Kafka {@link KafkaSerializationSchema} to serialize {@link GameEvent}s as JSON.
 *
 */

public class GameEventSerializationSchema implements KafkaSerializationSchema<GameEvent>{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private String topic;

    public GameEventSerializationSchema() {

    }

    public GameEventSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            final GameEvent message, @Nullable final Long timestamp
    ) {
        try {
            return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(message));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + message, e);
        }
    }
}

