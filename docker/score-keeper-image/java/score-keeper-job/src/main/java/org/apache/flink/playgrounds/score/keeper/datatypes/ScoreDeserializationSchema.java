package org.apache.flink.playgrounds.score.keeper.datatypes;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class ScoreDeserializationSchema implements DeserializationSchema<Score> {

    private static final long serialVersionUID = 1L;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Score deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Score.class);
    }

    @Override
    public boolean isEndOfStream(Score nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Score> getProducedType() {
        return TypeInformation.of(Score.class);
    }
}
