package org.apache.flink.playgrounds.score.keeper.datatypes;

import java.io.Serializable;
import java.util.Objects;

public class ProcessedScore implements Serializable {

    private long leaderboardsId;
    private char side;
    private float score;
    private long windowEndTime;
    private short leaderboardsType;
    private long entityId;
    private long eventTime;

    public ProcessedScore() {}

    public ProcessedScore(final long leaderboardsId, final char side,
                          final float score, final long windowEndTime,
                          final short leaderboardsType, final long entityId,
                          final long eventTime) {
        this.leaderboardsId = leaderboardsId;
        this.side = side;
        this.score = score;
        this.windowEndTime = windowEndTime;
        this.leaderboardsType = leaderboardsType;
        this.entityId = entityId;
        this.eventTime = eventTime;
    }

    public long getLeaderboardsId() {
        return leaderboardsId;
    }

    public void setLeaderboardsId(long leaderboardsId) {
        this.leaderboardsId = leaderboardsId;
    }

    public char getSide() {
        return side;
    }

    public void setSide(char side) {
        this.side = side;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public long getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(long windowEndTime) {
        this.windowEndTime = windowEndTime;
    }

    public short getLeaderboardsType() {
        return leaderboardsType;
    }

    public void setLeaderboardsType(short leaderboardsType) {
        this.leaderboardsType = leaderboardsType;
    }

    public long getEntityId() {
        return entityId;
    }

    public void setEntityId(long entityId) {
        this.entityId = entityId;
    }

    public long getEventTime() { return eventTime; }

    public void setEventTime(long eventTime) { this.eventTime = eventTime; }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final ProcessedScore that = (ProcessedScore) other;
        return leaderboardsId == that.leaderboardsId &&
                leaderboardsType == that.leaderboardsType &&
                entityId == that.entityId &&
                windowEndTime == that.windowEndTime &&
                score == that.score &&
                side == that.side &&
                eventTime == that.eventTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                leaderboardsId, side, score, windowEndTime,
                leaderboardsType, entityId, eventTime
        );
    }

    @Override
    public String toString() {
        return leaderboardsId + " @" + eventTime +
                ": (side) " + side +
                " (score) " + score +
                " (windowEndTime) " + windowEndTime +
                " (leaderboardsType) " + leaderboardsType +
                " (entityId) " + entityId;
    }
}
