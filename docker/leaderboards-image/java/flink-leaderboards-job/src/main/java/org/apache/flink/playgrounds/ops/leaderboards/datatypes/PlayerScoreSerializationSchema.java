package org.apache.flink.playgrounds.ops.leaderboards.datatypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * A Kafka {@link KafkaSerializationSchema} to serialize {@link PlayerScores}s as JSON.
 *
 */
public class PlayerScoreSerializationSchema implements KafkaSerializationSchema<PlayerScores> {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private String topic;

    public PlayerScoreSerializationSchema() {

    }

    public PlayerScoreSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            final PlayerScores message, @Nullable final Long timestamp
    ) {
        try {
            return new ProducerRecord<>(topic, objectMapper.writeValueAsBytes(message));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + message, e);
        }
    }
}

