package org.apache.flink.playgrounds.ops.leaderboards.utils;

import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEvent;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Base for all exercises with a few helper methods.
 */
public class ExerciseBase {
    public static SourceFunction<GameEvent> gameEvents = null;
    public static SourceFunction<String> strings = null;
    public static SinkFunction out = null;
    public static int parallelism = 4;

    /**
     * Retrieves a test source during unit tests and the given one during normal execution.
     */
    public static SourceFunction<GameEvent> gameEventSourceOrTest(SourceFunction<GameEvent> source) {
        if (gameEvents == null) {
            return source;
        }
        return gameEvents;
    }

    /**
     * Retrieves a test source during unit tests and the given one during normal execution.
     */
    public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
        if (strings == null) {
            return source;
        }
        return strings;
    }

    /**
     * Prints the given data stream during normal execution and collects outputs during tests.
     */
    public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }
}