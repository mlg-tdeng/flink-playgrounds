package org.apache.flink.playgrounds.score.keeper.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.playgrounds.score.keeper.datatypes.Score;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;


public class ScoreProcessFunction extends KeyedProcessFunction<Long, Score, Iterable<Score>> {

    private ListState<Score> scoresState;
    private final long durationMsec;

    public ScoreProcessFunction(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    public void open(Configuration config) {
        ListStateDescriptor<Score> scoresStateDescriptor =
                new ListStateDescriptor<Score>("buffered scores", Score.class);

        scoresState = getRuntimeContext().getListState(scoresStateDescriptor);
    }

    @Override
    public void processElement(Score score, Context context, Collector<Iterable<Score>> collector) throws Exception {
        Iterable<Score> existingScores = scoresState.get();
        long eventTime = score.getEventTime();
        TimerService timerService = context.timerService();

        if (existingScores == null) {
            // First score of this key is found. start the trigger of timer
            // Round up eventTime to the end of the window containing this event.
            long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

            // Schedule a callback for when the window has been completed.
            timerService.registerEventTimeTimer(endOfWindow);
        }

        // Add score to list state
        scoresState.add(score);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Iterable<Score>> out) throws Exception {
        Iterable<Score> bufferedScores = scoresState.get();

        out.collect(bufferedScores);

        // Clear buffered scores after it's collected
        scoresState.clear();
    }
}
