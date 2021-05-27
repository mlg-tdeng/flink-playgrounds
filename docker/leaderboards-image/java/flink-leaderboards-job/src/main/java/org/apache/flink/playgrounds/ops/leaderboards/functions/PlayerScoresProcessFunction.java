package org.apache.flink.playgrounds.ops.leaderboards.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.GameEvent;
import org.apache.flink.playgrounds.ops.leaderboards.datatypes.PlayerScores;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class PlayerScoresProcessFunction extends KeyedProcessFunction<Long, GameEvent, PlayerScores> {
    private ValueState<PlayerScores> playerScoresState;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<PlayerScores> playerScoresStateDescriptor =
                new ValueStateDescriptor<>("player scores", PlayerScores.class);

        playerScoresState = getRuntimeContext().getState(playerScoresStateDescriptor);
    }

    @Override
    public void processElement(GameEvent gameEvent, Context context, Collector<PlayerScores> out) throws Exception {
        PlayerScores playerPreviousScores = playerScoresState.value();


        if (playerPreviousScores == null) {
            // log player's scores initially at its event
            PlayerScores playerInitScores;
            if (gameEvent.isStart) {
                playerInitScores = new PlayerScores(gameEvent.playerId, gameEvent.gameFranchiseId,
                        gameEvent.teamId, gameEvent.totalKills, 0, 0);

            } else {
                // end event of this player comes before start event
                playerInitScores = new PlayerScores(gameEvent.playerId, gameEvent.gameFranchiseId,
                        gameEvent.teamId, gameEvent.totalKills, gameEvent.win == 2 ? 1 : 0, 1);

            }
            playerScoresState.update(playerInitScores);
        } else {
            // sum up total game played and kills
            if (!gameEvent.isStart) {
                // sum up total game played
                 playerPreviousScores.setTotalGamePlayed(playerPreviousScores.getTotalGamePlayed() + 1);

                // sum up total wins
                if (gameEvent.win == 2) {
                    playerPreviousScores.setTotalWins(playerPreviousScores.getTotalWins() + 1);
                }

                // sum up total kills
                playerPreviousScores.setTotalKills(gameEvent.totalKills);
            }
            playerScoresState.update(playerPreviousScores);
        }
        out.collect(playerScoresState.value());
    }
}
