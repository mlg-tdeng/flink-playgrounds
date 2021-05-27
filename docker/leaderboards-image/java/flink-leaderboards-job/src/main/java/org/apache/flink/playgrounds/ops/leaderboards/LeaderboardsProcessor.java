package org.apache.flink.playgrounds.ops.leaderboards;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEvent;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEventDeserializationSchema;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEventSerializationSchema;
import org.apache.flink.playgrounds.ops.leaderboards.utils.ExerciseBase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Properties;

/**
 * Flink job to process game event data into leaderboards.
 *
 */
public class LeaderboardsProcessor extends ExerciseBase {

    public static final String CHECKPOINTING_OPTION = "checkpointing";
//    public static final String EVENT_TIME_OPTION = "event-time";
    public static final String BACKPRESSURE_OPTION = "backpressure";
    public static final String OPERATOR_CHAINING_OPTION = "chaining";

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up stream env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        configureEnvironment(params, env);

        boolean inflictBackpressure = params.has(BACKPRESSURE_OPTION);

        String inputTopic = params.get("input-topic", "input");
        String outputTopic = params.get("output-topic", "output");
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "leaderboards");

        // start the source data generator
//        DataStream<GameEvent> gameEvent = env.addSource(gameEventSourceOrTest(new GameEventSourceGenerator()));
//        printOrTest(gameEvent);

        System.out.print("Kafka properties are set up and start to create DataStream. ");

        DataStream<GameEvent> gameEvents =
                env.addSource(new FlinkKafkaConsumer<>(inputTopic, new GameEventDeserializationSchema(), kafkaProps))
                        .name("GameEvent Source")
                        // ## assign watermark
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<GameEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                            .withTimestampAssigner((event, timestamp) -> event.getEventTime()));

        DataStream<GameEvent> leaderboards = gameEvents.keyBy(e -> e.getPlayerId())
        .map(new Enrichment());

        leaderboards
                .addSink(new FlinkKafkaProducer<>(
                        outputTopic,
                        new GameEventSerializationSchema(outputTopic),
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Leaderboards Sink");

        env.execute("Game Events to Leaderboards!");
    }

    public static class Enrichment implements MapFunction<GameEvent, GameEvent> {
        @Override
        public GameEvent map(GameEvent gameEvent) throws Exception {
            System.out.print("processing game event");
            return gameEvent;
        }
    }

    private static void configureEnvironment(
            final ParameterTool params,
            final StreamExecutionEnvironment env
    ) {
        boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
//        boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);
        boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

        if (checkpointingEnabled) {
            env.enableCheckpointing(1000);
        }

//        if (eventTimeSemantics) {
//            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        }

        if(!enableChaining){
            //disabling Operator chaining to make it easier to follow the Job in the WebUI
            env.disableOperatorChaining();
        }
    }
}