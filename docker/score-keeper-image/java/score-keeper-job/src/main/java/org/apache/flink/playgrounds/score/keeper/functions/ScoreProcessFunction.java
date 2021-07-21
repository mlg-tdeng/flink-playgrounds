package org.apache.flink.playgrounds.score.keeper.functions;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.playgrounds.score.keeper.datatypes.ProcessedScore;
import org.apache.flink.playgrounds.score.keeper.datatypes.Score;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

public class ScoreProcessFunction extends KeyedProcessFunction<Long, Score, ProcessedScore> {

    private transient ValueState<Character> sideState;
    private transient ValueState<Long> lastWindowEndTimeState;
    private final long durationMsec;

    public ScoreProcessFunction(Time duration) {
        this.durationMsec = duration.toMilliseconds();
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Character> sideStateDescriptor =
                new ValueStateDescriptor<Character>("side", Character.class);
        sideState = getRuntimeContext().getState(sideStateDescriptor);

        ValueStateDescriptor<Long> lastWindowEndTimeDescriptor =
                new ValueStateDescriptor<Long>("last window end time", Long.class);
        lastWindowEndTimeState = getRuntimeContext().getState(lastWindowEndTimeDescriptor);
    }

    @Override
    public void processElement(Score score, Context context, Collector<ProcessedScore> collector) throws Exception {
        long eventTime = score.getEventTime();
        // Round up eventTime to the end of the window containing this event.
        long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

        TimerService timerService = context.timerService();

        Character currentSide = sideState.value();
        if (currentSide == null) {
            // First score for this key is arrived. pick a side
            sideState.update('A');
            timerService.registerEventTimeTimer(endOfWindow);
        }

        Long lastWindowEndTime = lastWindowEndTimeState.value();
        if ( lastWindowEndTime == null) {
            // If no last window end time state, create one
            lastWindowEndTimeState.update(endOfWindow);
        } else {
            if (endOfWindow != lastWindowEndTime.longValue()) {
                // If this element's calculated doesn't equal to last saved one, trigger a timer
                System.out.println("-------------- new window end time, start to register a new timer " + endOfWindow);
                timerService.registerEventTimeTimer(endOfWindow);

                lastWindowEndTimeState.update(endOfWindow);
            }
        }

        // Create a new process score instance to send it out
        System.out.println("--------------- Current Side of Processed Score " + sideState.value());
        ProcessedScore processedScore = new ProcessedScore(score.getLeaderboardsId(),
                sideState.value(), score.getScore(), endOfWindow, score.getLeaderboardsType(), score.getEntityId(), score.getEventTime());

        if (processedScore.getLeaderboardsId() == 2021100000) {
            collector.collect(processedScore);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<ProcessedScore> out) throws Exception {
        System.out.println("-------------- current key: " + context.getCurrentKey());
        System.out.println("-------------- onTimer is called at " + timestamp);
        System.out.println("-------------- current end timestamp: " + context.timestamp());
        System.out.println("-------------- current side: " + sideState.value() + ". Flipping side...");

        // Flip side at timer callback
        if (sideState.value() == 'A') {
            sideState.update('B');
        } else {
            sideState.update('A');
        }

        System.out.println("-------------- Flipping done. current side: " + sideState.value() );
    }
}
