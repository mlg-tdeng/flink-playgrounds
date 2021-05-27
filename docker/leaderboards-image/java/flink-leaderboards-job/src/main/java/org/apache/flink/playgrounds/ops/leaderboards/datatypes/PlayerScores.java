package org.apache.flink.playgrounds.ops.leaderboards.datatypes;

import java.util.Objects;

public class PlayerScores {
    private long playerId;
    private long gameFranchiseId;
    private long teamId;
    private long totalWins = 0;
    private long totalKills = 0;
    private long totalGamePlayed = 0;

    PlayerScores() {}

    public PlayerScores(final long playerId, final long gameFranchiseId, final long teamId,
                 final long totalKills, final long totalWins, final long totalGamePlayed) {
        this.playerId = playerId;
        this.gameFranchiseId = gameFranchiseId;
        this.teamId = teamId;
        this.totalKills = totalKills;
        this.totalWins = totalWins;
        this.totalGamePlayed = totalGamePlayed;
    }

    public long getPlayerId() { return playerId; }

    public void setPlayerId(final long playerId) { this.playerId = playerId; }

    public long getGameFranchiseId() { return gameFranchiseId; }

    public void setGameFranchiseId(final long gameFranchiseId) { this.gameFranchiseId = gameFranchiseId; }

    public long getTeamId() {return teamId; }

    public void setTeamId(final long teamId) { this.teamId = teamId; }

    public long getTotalWins() { return totalWins; }

    public void setTotalWins(final long totalWins) { this.totalWins = totalWins; }

    public long getTotalKills() { return totalKills; }

    public void setTotalKills(final long totalKills) { this.totalKills = totalKills; }

    public long getTotalGamePlayed() { return totalGamePlayed; }

    public void setTotalGamePlayed(final long totalGamePlayed) { this.totalGamePlayed = totalGamePlayed; }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final PlayerScores that = (PlayerScores) other;
        return playerId == that.playerId &&
                teamId == that.teamId &&
                gameFranchiseId == that.gameFranchiseId &&
                totalWins == that.totalWins &&
                totalGamePlayed == totalGamePlayed &&
                totalKills == that.totalKills;

    }

    @Override
    public int hashCode() {
        return Objects.hash(
                playerId,
                gameFranchiseId,
                teamId,
                totalWins,
                totalKills,
                totalGamePlayed
        );
    }

    @Override
    public String toString() {
        return playerId + ": " +
                "(team) " + teamId + " " +
                "(gameFranchise) " + gameFranchiseId + " " +
                "(totalWins) " + totalWins + " " +
                "(totalKills) " + totalKills + " " +
                "(totalGamePlayed) " + totalGamePlayed;
    }
}
