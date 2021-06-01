package org.apache.flink.playgrounds.ops.leaderboards;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.*;
import org.apache.flink.playgrounds.ops.leaderboards.functions.BackpressureMap;
import org.apache.flink.playgrounds.ops.leaderboards.functions.PlayerScoresProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
public class LeaderboardsProcessor {

    public static final String CHECKPOINTING_OPTION = "checkpointing";
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

        FlinkKafkaConsumer<GameEvent> myConsumer =
        new FlinkKafkaConsumer<>(inputTopic, new GameEventDeserializationSchema(), kafkaProps);

        DataStream<GameEvent> gameEvents = env
            .addSource(myConsumer
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20))
//                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                        )).name("GameEvent Source");

        if (inflictBackpressure) {
            // Force a network shuffle so that the backpressure will affect the buffer pools
            gameEvents = gameEvents
                    .keyBy(GameEvent::getPlayerId)
                    .map(new BackpressureMap())
                    .name("Backpressure");
        }

        DataStream<PlayerScores> playerScores = gameEvents.keyBy(e -> e.getPlayerId())
        .process(new PlayerScoresProcessFunction())
                .name("Process Game Events to Player Scores");



        playerScores
                .addSink(new FlinkKafkaProducer<>(
                        outputTopic,
                        new PlayerScoreSerializationSchema(outputTopic),
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name("Player Scores Sink");

        env.execute("Game Events to Player Scores!");
    }

    public static class Enrichment implements MapFunction<GameEvent, GameEvent> {
        @Override
        public GameEvent map(GameEvent gameEvent) throws Exception {
            return gameEvent;
        }
    }

    private static void configureEnvironment(
            final ParameterTool params,
            final StreamExecutionEnvironment env
    ) {
        boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
        boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

        if (checkpointingEnabled) {
            env.enableCheckpointing(1000);
        }

        if(!enableChaining){
            //disabling Operator chaining to make it easier to follow the Job in the WebUI
            env.disableOperatorChaining();
        }
    }
}