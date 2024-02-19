package edu.cut.smacc.server.statistics;

import edu.cut.smacc.server.statistics.type.StatisticType;
import edu.cut.smacc.server.statistics.type.general.TierGeneralStatistics;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

public class TierGeneralStatisticsTest {

    @Test
    void testTierStatistics() {
        // Create statistics for memory and disk
        TierGeneralStatistics memStatistics = new TierGeneralStatistics();
        TierGeneralStatistics diskStatistics = new TierGeneralStatistics();

        // Every stat in both tier statistics should be 0
        for (StatisticType stat : StatisticType.listTierStats()) {
            assert memStatistics.getStat(stat).longValue() == 0;
            assert diskStatistics.getStat(stat).longValue() == 0;
        }

        System.out.println("TierStatisticsTest.testTierStatistics() passed.");
    }

    @Test
    void testIncrementStatBy() {
        // Create statistics for memory and disk
        TierGeneralStatistics memStatistics = new TierGeneralStatistics();
        TierGeneralStatistics diskStatistics = new TierGeneralStatistics();

        // Increment the number of cache files in memory by 10
        memStatistics.incrementStatBy(StatisticType.CACHE_FILES, 10);
        assert memStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(10)).longValue();
        assert diskStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(0)).longValue();

        // Increment the number of cache files in disk by 20
        diskStatistics.incrementStatBy(StatisticType.CACHE_FILES, 20);
        assert memStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(10)).longValue();
        assert diskStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(20)).longValue();
        // Use increment() method
        memStatistics.incrementStat(StatisticType.CACHE_FILES);
        assert memStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(11)).longValue();
        diskStatistics.incrementStat(StatisticType.CACHE_FILES);
        assert diskStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(21)).longValue();

        // Increment every other stat by 5, using incrementStatBy()
        for (StatisticType stat : StatisticType.values()) {
            if (stat != StatisticType.CACHE_FILES) {
                memStatistics.incrementStatBy(stat, 5);
                diskStatistics.incrementStatBy(stat, 5);
            }
        }
        // Check that everything except CACHE_FILES is 5
        for (StatisticType stat : StatisticType.values()) {
            if (stat != StatisticType.CACHE_FILES) {
                assert memStatistics.getStat(stat).longValue() == (new AtomicLong(5)).longValue();
                assert diskStatistics.getStat(stat).longValue() == (new AtomicLong(5)).longValue();
            }
        }

        System.out.println("TierStatisticsTest.testIncrementStatBy() passed.");
    }

    @Test
    void testDecrementStatBy() {
        // Create statistics for memory and disk
        TierGeneralStatistics memStatistics = new TierGeneralStatistics();

        // Increment the number of cache files in memory by 10
        memStatistics.incrementStatBy(StatisticType.CACHE_FILES, 10);
        assert memStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(10)).longValue();
        // Decrement it by 5
        memStatistics.decrementStatBy(StatisticType.CACHE_FILES, 5);
        assert memStatistics.getStat(StatisticType.CACHE_FILES).longValue() == (new AtomicLong(5)).longValue();
        // Increment everything by 1, except CACHE_FILES
        for (StatisticType stat : StatisticType.values()) {
            if (stat != StatisticType.CACHE_FILES) {
                memStatistics.incrementStat(stat);
            }
        }
        // Check that everything except CACHE_FILES is 1
        for (StatisticType stat : StatisticType.values()) {
            assert stat == StatisticType.CACHE_FILES || memStatistics.getStat(stat).longValue() == (new AtomicLong(1)).longValue();
        }
        // Decrement everything by 1
        for (StatisticType stat : StatisticType.values()) {
            memStatistics.decrementStat(stat);
        }
        // Check that everything except CACHE_FILES is 0
        for (StatisticType stat : StatisticType.values()) {
            assert stat == StatisticType.CACHE_FILES || memStatistics.getStat(stat).longValue() == (new AtomicLong(0)).longValue();
        }

        System.out.println("TierStatisticsTest.testDecrementStatBy() passed.");
    }

    @Test
    void testClearStats() {
        // Create statistics for memory and disk
        TierGeneralStatistics memStatistics = new TierGeneralStatistics();
        TierGeneralStatistics diskStatistics = new TierGeneralStatistics();

        // Check that every stat is set at 0
        assert memStatistics.getStatCount() == StatisticType.listTierStats().size();
        assert diskStatistics.getStatCount() == StatisticType.listTierStats().size();
        for (StatisticType stat : StatisticType.listTierStats()) {
            assert memStatistics.getStat(stat).longValue() == (new AtomicLong(0)).longValue();
            assert diskStatistics.getStat(stat).longValue() == (new AtomicLong(0)).longValue();
        }
        // Clear the stats
        memStatistics.clearStats();
        diskStatistics.clearStats();
        // Check that every stat is 0
        assert memStatistics.getStatCount() == 0;
        assert diskStatistics.getStatCount() == 0;

        System.out.println("TierStatisticsTest.testClearStats() passed.");
    }

    @Test
    void testResetStats() {
        // Create statistics for memory and disk
        TierGeneralStatistics memStatistics = new TierGeneralStatistics();
        TierGeneralStatistics diskStatistics = new TierGeneralStatistics();

        // Increment every stat by 10
        for (StatisticType stat : StatisticType.values()) {
            memStatistics.incrementStatBy(stat, 10);
            diskStatistics.incrementStatBy(stat, 10);
        }
        // Check that every stat is 10
        for (StatisticType stat : StatisticType.values()) {
            assert memStatistics.getStat(stat).longValue() == (new AtomicLong(10)).longValue();
            assert diskStatistics.getStat(stat).longValue() == (new AtomicLong(10)).longValue();
        }
        // Reset the stats
        memStatistics.resetStats();
        diskStatistics.resetStats();
        // Check that every stat is 0
        for (StatisticType stat : StatisticType.values()) {
            assert memStatistics.getStat(stat).longValue() == (new AtomicLong(0)).longValue();
            assert diskStatistics.getStat(stat).longValue() == (new AtomicLong(0)).longValue();
        }

        System.out.println("TierStatisticsTest.testResetStats() passed.");
    }

}
