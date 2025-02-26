package org.apache.flink.playgrounds.score.keeper.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.playgrounds.score.keeper.datatypes.Score;

import java.time.LocalTime;

/**
 * This MapFunction causes severe backpressure during even-numbered minutes.
 * E.g., from 10:12:00 to 10:12:59 it will only process 10 events/sec,
 * but from 10:13:00 to 10:13:59 events will pass through unimpeded.
 */
public class BackpressureMap implements MapFunction<Score, Score> {

    private boolean causeBackPressure() {
        return ((LocalTime.now().getMinute() % 2) == 0);
    }

    @Override
    public Score map(Score score) throws Exception {
        if (causeBackPressure()) {
            Thread.sleep(100);
        }

        return score;
    }
}
