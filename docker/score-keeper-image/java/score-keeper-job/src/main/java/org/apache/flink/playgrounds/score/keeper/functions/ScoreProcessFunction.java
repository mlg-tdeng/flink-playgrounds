package org.apache.flink.playgrounds.score.keeper.functions;

import org.apache.flink.playgrounds.score.keeper.datatypes.Score;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ScoreProcessFunction extends KeyedProcessFunction<Long, Score, Score> {

    @Override
    public void processElement(Score score, Context context, Collector<Score> collector) throws Exception {

    }
}
