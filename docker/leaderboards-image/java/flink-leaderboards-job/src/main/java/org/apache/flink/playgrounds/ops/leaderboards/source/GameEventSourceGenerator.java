package org.apache.flink.playgrounds.ops.leaderboards.source;

import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEvent;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * This SourceFunction generates a data stream of Game Event records that include event time
 * timestamps.
 *
 * <p>The stream is produced out-of-order, and includes Watermarks (with no late events).
 *
 */
public class GameEventSourceGenerator implements SourceFunction<GameEvent> {
    private static boolean running = true;
    public static final int SLEEP_MILLIS_PER_EVENT = 200;
    private static final int BATCH_SIZE = 5;

    @Override
    public void run(SourceContext<GameEvent> ctx) throws Exception {
        PriorityQueue<GameEvent> endEventQ = new PriorityQueue<>(100);
        long id = 0;
        long maxStartTime = 0;

        while (running) {
            // generate a batch of START events
            List<GameEvent> startEvents = new ArrayList<GameEvent>(BATCH_SIZE);
            for (int i = 1; i <= BATCH_SIZE; i++) {
                GameEvent gameEvent = new GameEvent(id + i, true);
                startEvents.add(gameEvent);
                // the start times may be in order, but let's not assume that
                maxStartTime = Math.max(maxStartTime, gameEvent.startTime);
            }

            // enqueue the corresponding END events
            for (int i = 1; i <= BATCH_SIZE; i++) {
                endEventQ.add(new GameEvent(id + i, false));
            }

            // release the END events coming before the end of this new batch
            // (this allows a few END events to precede their matching START event)
            while (endEventQ.peek().getEventTime() <= maxStartTime) {
                GameEvent gameEvent = endEventQ.poll();
                ctx.collectWithTimestamp(gameEvent, gameEvent.getEventTime());
            }

            // then emit the new START events (out-of-order)
            java.util.Collections.shuffle(startEvents, new Random(id));
            startEvents.iterator().forEachRemaining(r -> ctx.collectWithTimestamp(r, r.getEventTime()));

            // produce a Watermark timestamp
            ctx.emitWatermark(new Watermark(maxStartTime));

            // prepare for the next batch
            id += BATCH_SIZE;

            // don't go too fast
            Thread.sleep(BATCH_SIZE * SLEEP_MILLIS_PER_EVENT);
        }
    }

    @Override
    public void cancel() {running = false;}
}
