package org.apache.flink.playgrounds.score.keeper.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.playgrounds.score.keeper.datatypes.Score;
import org.apache.flink.playgrounds.score.keeper.datatypes.ScoreSerializationSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Properties;

public class ScoreKafkaGenerator {

    static final int BATCH_SIZE = 5;
    public static final int SLEEP_MILLIS_PER_EVENT = 200;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");

        Properties kafkaProps = createKafkaProperties(params);

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);
        long id = 0;

        while (true) {
            // generate a batch of scores
            for (int i = 1; i <= BATCH_SIZE; i++) {
                Score score = new Score(id + i);

                ProducerRecord<byte[], byte[]> record = new ScoreSerializationSchema(topic).serialize(
                        score, null
                );

                producer.send(record);
            }

            // prepare for the next batch
            id += BATCH_SIZE;

            // don't go too fast
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }
    }

    private static Properties createKafkaProperties(ParameterTool params) {
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }
}
