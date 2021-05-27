package org.apache.flink.playgrounds.ops.leaderboards.datatypes;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * A Kafka {@link DeserializationSchema} to deserialize {@link GameEvent}s from JSON.
 *
 */
public class GameEventDeserializationSchema implements DeserializationSchema<GameEvent> {
    private static final long serialVersionUID = 1L;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public GameEvent deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, GameEvent.class);
    }

    @Override
    public boolean isEndOfStream(GameEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<GameEvent> getProducedType() {
        return TypeInformation.of(GameEvent.class);
    }

}
