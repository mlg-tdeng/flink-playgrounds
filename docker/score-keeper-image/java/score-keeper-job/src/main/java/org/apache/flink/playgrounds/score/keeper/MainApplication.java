package org.apache.flink.playgrounds.score.keeper;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.playgrounds.score.keeper.datatypes.ProcessedScore;
import org.apache.flink.playgrounds.score.keeper.datatypes.ProcessedScoreSerializationSchema;
import org.apache.flink.playgrounds.score.keeper.datatypes.Score;
import org.apache.flink.playgrounds.score.keeper.datatypes.ScoreDeserializationSchema;
import org.apache.flink.playgrounds.score.keeper.functions.BackpressureMap;
import org.apache.flink.playgrounds.score.keeper.functions.ScoreProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Duration;
import java.util.Properties;

public class MainApplication {
    public static final String CHECKPOINTING_OPTION = "checkpointing";
    public static final String BACKPRESSURE_OPTION = "backpressure";
    public static final String OPERATOR_CHAINING_OPTION = "chaining";

    /**
     * Main method
     *
     * @throws Exception which occurs during the job execution
     */
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up stream env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        configureEnvironment(params, env);

        boolean inflictBackpressure = params.has(BACKPRESSURE_OPTION);

        // obtain config for input Kafka
        String inputTopic = params.get("input_topic", "input");
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "score_keeper");

        // add Kafka consumer
        FlinkKafkaConsumer<Score> myConsumer = new FlinkKafkaConsumer<Score>(inputTopic, new ScoreDeserializationSchema(), kafkaProps);

        // 1. Source
        DataStream<Score> scores = env
                .addSource(myConsumer
                        // Set up event time processing and watermark strategy
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Score>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((score, timestamp) -> score.getEventTime())
                        ))
                .name("Kafka Score Source");

        if (inflictBackpressure) {
            // Force a network shuffle so that the backpressure will affect the buffer pools
            scores = scores
                    .keyBy(Score::getLeaderboardsId)
                    .map(new BackpressureMap())
                    .name("Backpressure");
        }

        // 2. Process
        DataStream<ProcessedScore> scoresKeeper = scores
                .keyBy(e -> e.getLeaderboardsId())
//                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new ScoreProcessFunction(Time.minutes(1)))
                .name("Process Scores");

        // 3. Sink
        scoresKeeper
                .addSink(new FlinkKafkaProducer<>(
                    "output",
                    new ProcessedScoreSerializationSchema(),
                    kafkaProps,
                    FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        )).name("Kafka Sink");

        env.execute("Score Keeper!");
    }

    private static void configureEnvironment(ParameterTool params, StreamExecutionEnvironment env) {
        boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
        boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

        if (checkpointingEnabled) {
            env.enableCheckpointing(1000);
        }

        if (!enableChaining) {
            env.disableOperatorChaining();
        }
    }
}
