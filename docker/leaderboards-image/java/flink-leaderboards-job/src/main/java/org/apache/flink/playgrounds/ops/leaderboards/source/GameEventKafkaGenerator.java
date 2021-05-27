package org.apache.flink.playgrounds.ops.leaderboards.source;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEvent;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEventSerializationSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

public class GameEventKafkaGenerator {
    static final int BATCH_SIZE = 5;
    public static final int SLEEP_MILLIS_PER_EVENT = 200;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "input");

        Properties kafkaProps = createKafkaProperties(params);

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);

        PriorityQueue<GameEvent> endEventQ = new PriorityQueue<>(100);
        long id = 0;
        long maxStartTime = 0;

        while (true) {
            // generate a batch of START events
            List<GameEvent> startEvents = new ArrayList<>(BATCH_SIZE);
            for (int i = 1; i <= BATCH_SIZE; i++) {
                GameEvent gameEvent = new GameEvent(id + i, true);
                startEvents.add(gameEvent);
                // the start times may be in order, but let's not assume that
                maxStartTime = Math.max(maxStartTime, gameEvent.startTime.toEpochMilli());
            }

            // enqueue the corresponding END events
            for (int i = 1; i <= BATCH_SIZE; i++) {
                endEventQ.add(new GameEvent(id + i, false));
            }

            while (endEventQ.peek().getEventTime() <= maxStartTime) {
                GameEvent gameEvent = endEventQ.poll();

                ProducerRecord<byte[], byte[]> record = new GameEventSerializationSchema(topic).serialize(
                        gameEvent, null
                );

                producer.send(record);
            }

            // then emit the new START events (out-of-order)
            java.util.Collections.shuffle(startEvents, new Random(id));
            startEvents.iterator().forEachRemaining(gameEvent -> {
                ProducerRecord<byte[], byte[]> record = new GameEventSerializationSchema(topic).serialize(
                        gameEvent, null
                );

                producer.send(record);
            });

            // prepare for the next batch
            id += BATCH_SIZE;

            // don't go too fast
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }
    }


    private static Properties createKafkaProperties(final ParameterTool params) {
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }
}
