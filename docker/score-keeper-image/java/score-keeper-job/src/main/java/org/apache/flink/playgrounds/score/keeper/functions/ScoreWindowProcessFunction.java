package org.apache.flink.playgrounds.score.keeper.functions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.playgrounds.score.keeper.datatypes.Score;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class ScoreWindowProcessFunction extends ProcessWindowFunction<
        Score, Tuple3<Long, Long, Score>, Long, TimeWindow> {

    @Override
    public void process(Long leaderboardsId,
                        Context context,
                        Iterable<Score> scores,
                        Collector<Tuple3<Long, Long, Score>> out) throws Exception {

        for (Score score : scores) {
            // collect scores of this leaderboardsId to the same output

        }
//        out.collect(Tuple3.of(leaderboardsId, context.window().getEnd(), scores));
    }
}
