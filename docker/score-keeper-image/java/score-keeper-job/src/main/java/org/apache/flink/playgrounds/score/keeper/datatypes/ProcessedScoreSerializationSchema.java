package org.apache.flink.playgrounds.score.keeper.datatypes;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class ProcessedScoreSerializationSchema implements KafkaSerializationSchema<ProcessedScore> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ProcessedScoreSerializationSchema() {

    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            final ProcessedScore message, @Nullable final Long timestamp
    ) {
        try {
            // Send message to A or B side topic according to what's specified in message
            return new ProducerRecord<>("output_"+ message.getSide(), objectMapper.writeValueAsBytes(message));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + message, e);
        }
    }
}
