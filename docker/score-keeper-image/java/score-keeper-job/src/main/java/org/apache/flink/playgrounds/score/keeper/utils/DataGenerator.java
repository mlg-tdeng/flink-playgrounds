package org.apache.flink.playgrounds.score.keeper.utils;

import java.time.Instant;
import java.util.Random;

public class DataGenerator {

    private static final int NUMBER_OF_LEADERBOARDS = 5;
    private static final int NUMBER_OF_ENTITIES = 1000;
    private static final int SECONDS_BETWEEN_ENTITIES = 20;
    private static final Instant beginTime = Instant.parse("2021-01-01T12:00:00.00Z");

    private transient long seed;

    /**
     * Create a DataGenerator for the specified leaderboardsId
     * @param seed
     */
    public DataGenerator(long seed) {
        this.seed = seed;
    }

    public long leaderboardsId() {
        Random rnd = new Random(seed);
        return 2021100000 + rnd.nextInt(NUMBER_OF_LEADERBOARDS);
    }

    /**
     * Deterministically generates entityId
     * @return
     */
    public long entityId() {
        Random rnd = new Random(seed);
        return 2021000000 + rnd.nextInt(NUMBER_OF_ENTITIES);
    }

    public Instant eventTime() {
        return beginTime.plusSeconds(SECONDS_BETWEEN_ENTITIES * seed);
    }

    public short leaderboardsType() {
        return (short) aLong(1L, 3L);
    }

    public float score() {
        return aLong(0L, 300L, 10F, 15F);
    }

    public int snapshotInterval() {
        return (int) aLong(300L, 3000L, 1650L, 300L);
    }

    private long aLong(long min, long max) {
        float mean = (min + max) / 2.0F;
        float stddev = (max - min) / 8F;

        return aLong(min, max, mean, stddev);
    }

    private long aLong(long min, long max, float mean, float stddev) {
        Random rnd = new Random(seed);
        long value;
        do {
            value = (long) Math.round((stddev * rnd.nextGaussian()) + mean);
        } while ((value < min) || (value > max));
        return value;
    }

}
