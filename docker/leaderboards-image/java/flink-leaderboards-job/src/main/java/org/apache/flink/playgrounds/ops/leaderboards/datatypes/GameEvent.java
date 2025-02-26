package org.apache.flink.playgrounds.ops.leaderboards.datatypes;

import org.apache.flink.playgrounds.ops.leaderboards.utils.DataGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Instant;

/**
 * A Game Start and End Event.
 *
 * <p>It has these fields
 * - the start time or end time
 * - the gameId
 * - the playerId
 * - the teamId
 * - the gameFranchiseId
 * - the gameMode (0 - 1v1, 1 - 2v2, 2 - 3v3)
 * - the isStart
 * - win or lose or tie (0 - lose, 1 - tie, 2 - win)
 * - the total kills in the game
 */

@JsonIgnoreProperties({ "eventTime"})
public class GameEvent implements Comparable<GameEvent>, Serializable {

    public long totalKills;
    public short gameMode;
    public short win;
    public long gameFranchiseId;
    public long teamId;
    public boolean isStart;
    public long endTime;
    public long startTime;
    public long playerId;
    public long gameId;

    /**
     * Create a new GameEvent with now as start and end time.
     */
    public GameEvent() {
        this.startTime = Instant.now().toEpochMilli();
        this.endTime = Instant.now().toEpochMilli();
    }

    /**
     * Invent a GameEvent.
     */
    public GameEvent(long gameId, boolean isStart) {
        DataGenerator g = new DataGenerator(gameId);

        this.gameId = gameId;
        this.isStart = isStart;
        this.playerId = g.driverId();
        this.startTime = g.startTime().toEpochMilli();
        this.endTime = isStart ? Instant.ofEpochMilli(0).toEpochMilli() : g.endTime().toEpochMilli();
        this.teamId = g.teamId();
        this.gameFranchiseId = g.gameFranchiseId();
        this.gameMode = g.passengerCnt();
        this.win = isStart ? -1 : g.win();
        this.totalKills = isStart ? 0 : g.totalKills();
    }

    /**
     * Create a GameEvent with given parameters.
     */
    public GameEvent(long gameId, long playerId, boolean isStart, long gameFranchiseId, short gameMode,
                     long teamId, long startTime, long endTime, short win, long totalKills) {
        this.gameId = gameId;
        this.playerId = playerId;
        this.gameFranchiseId = gameFranchiseId;
        this.gameMode = gameMode;
        this.teamId = teamId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.isStart = isStart;
        this.win = win;
        this.totalKills = totalKills;
    }

    @Override
    public String toString() {
        return gameId + "," +
                (isStart ? "START" : "END") + "," +
                startTime + "," +
                endTime + "," +
                "(game)" + gameFranchiseId + "," +
                "(mode)" + gameMode + "," +
                "(player)" + playerId + "," +
                "(team)" + teamId + "," +
                "(win?)" + win + "," +
                "(total kills)" + totalKills;
    }

    /**
     * Compares this GameEvent with the given one.
     *
     * <ul>
     *     <li>sort by timestamp,</li>
     *     <li>putting START events before END events if they have the same timestamp</li>
     * </ul>
     */
    public int compareTo(@Nullable GameEvent other) {
        if (other == null) {
            return 1;
        }
        int compareTimes = Long.compare(this.getEventTime(), other.getEventTime());
        if (compareTimes == 0) {
            if (this.isStart == other.isStart) {
                return 0;
            }
            else {
                if (this.isStart) {
                    return -1;
                }
                else {
                    return 1;
                }
            }
        }
        else {
            return compareTimes;
        }
    }

    /**
     * Gets the ride's time stamp (start or end time depending on {@link #isStart}).
     */
    public long getEventTime() {
        if (isStart) {
            return startTime;
        }
        else {
            return endTime;
        }
    }

    public long getPlayerId() {
        return playerId;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof GameEvent &&
                this.gameId == ((GameEvent) other).gameId;
    }
}

