package org.apache.flink.playgrounds.score.keeper.datatypes;

import org.apache.flink.playgrounds.score.keeper.utils.DataGenerator;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;

/**
 * A Score Event
 *
 * <p>
 *     It has these fields
 *     - entity_id
 *     - leaderboards_id
 *     - score
 *     - leaderboards_type
 *     - snapshot_interval in sec
 *     - event_time
 * </p>
 */
public class Score implements Comparable<Score>, Serializable {

    public long entityId;
    public long leaderboardsId;
    public float score;
    public short leaderboardsType;
    public int snapshotInterval;
    public long eventTime;

    /**
     * Create a new Score with now
     */
    public Score() {
        this.eventTime = Instant.now().toEpochMilli();
    }

    /**
     * Invent a Score
     */
    public Score(long seed) {
        DataGenerator g = new DataGenerator(seed);

        this.leaderboardsId = g.leaderboardsId();
        this.entityId = g.entityId();
        this.eventTime = g.eventTime().toEpochMilli();
        this.leaderboardsType = g.leaderboardsType();
        this.score = g.score();
        this.snapshotInterval = g.snapshotInterval();
    }

    /**
     * Generate a Score with given parameters
     */
    public Score(long entityId, long leaderboardsId, float score,
                 short leaderboardsType, int snapshotInterval, long eventTime) {
        this.entityId = entityId;
        this.leaderboardsId = leaderboardsId;
        this.score = score;
        this.leaderboardsType = leaderboardsType;
        this.snapshotInterval = snapshotInterval;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return '@' + eventTime + ": " + entityId + "," +
                leaderboardsId + '(' + leaderboardsType + " every " + snapshotInterval
                + ") = " + score;
    }

    public int compareTo(@Nullable Score other) {
        if (other == null) {
            return 1;
        }
        int compareTime = Long.compare(this.getEventTime(), other.getEventTime());
        if (compareTime == 0) {
            if (this.getScore() > other.getScore()) {
                return 1;
            } else if (this.getScore() == other.getScore()) {
                return 0;
            } else {
                return -1;
            }
        } else {
            return compareTime;
        }
    }

    public float getScore() {
        return score;
    }

    public long getEventTime() {
        return eventTime;
    }

    public long getLeaderboardsId() {
        return leaderboardsId;
    }
}
