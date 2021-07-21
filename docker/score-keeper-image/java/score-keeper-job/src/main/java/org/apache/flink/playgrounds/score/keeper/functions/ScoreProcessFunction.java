package org.apache.flink.playgrounds.score.keeper.functions;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.playgrounds.score.keeper.datatypes.ProcessedScore;
import org.apache.flink.playgrounds.score.keeper.datatypes.Score;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ScoreProcessFunction extends KeyedProcessFunction<Long, Score, ProcessedScore> {

    private transient ValueState<Character> sideState;
    private transient ValueState<Boolean> eventArrivedState;
    private final long durationMsec;

    public ScoreProcessFunction(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Character> sideStateDescriptor =
                new ValueStateDescriptor<Character>("side", Character.class);
        sideState = getRuntimeContext().getState(sideStateDescriptor);

        ValueStateDescriptor<Boolean> eventArrivedStateDescriptor =
                new ValueStateDescriptor<Boolean>("event arrived", Boolean.class);
        eventArrivedState = getRuntimeContext().getState(eventArrivedStateDescriptor);
    }

    @Override
    public void processElement(Score score, Context context, Collector<ProcessedScore> collector) throws Exception {
        long eventTime = score.getEventTime();
        // Round up eventTime to the end of the window containing this event.
        long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

        TimerService timerService = context.timerService();

        System.out.println("-------------- register timer: " + score.toString() + " / for end time " + endOfWindow);
        timerService.registerEventTimeTimer(endOfWindow);

        Boolean hasEventArrived = eventArrivedState.value();

        if (hasEventArrived == null) {
            // First score for this key is arrived. pick a side
            eventArrivedState.update(true);
            sideState.update('A');
        }

        Character currentSide = sideState.value();

        // Create a new process score instance to send it out
        ProcessedScore processedScore = new ProcessedScore(score.getLeaderboardsId(),
                currentSide, score.getScore(), endOfWindow, score.getLeaderboardsType(), score.getEntityId());

        collector.collect(processedScore);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<ProcessedScore> out) throws Exception {
        System.out.println("-------------- current key" + context.getCurrentKey());
        System.out.println("-------------- onTimer is called at " + timestamp);
        System.out.println("-------------- current end timestamp" + context.timestamp());

        Character currentSide = sideState.value();

        // Flip side at timer callback
        if (currentSide == 'A') {
            sideState.update('B');
        } else {
            sideState.update('A');
        }
    }
}
