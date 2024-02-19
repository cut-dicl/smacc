package edu.cut.smacc.server.statistics;

import edu.cut.smacc.server.cache.common.StoreOptionType;
import edu.cut.smacc.server.statistics.type.StatisticType;
import edu.cut.smacc.server.statistics.type.Statistics;
import edu.cut.smacc.server.statistics.type.performance.PerformanceStatistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * This class is responsible for writing statistics to a tab-separated file.
 */
public class StatisticsWriter {

    private static final String TIMESTAMP = "TIMESTAMP";

    private final String filename;
    private Statistics globalStats;

    public StatisticsWriter(String filename) {
        this.filename = filename;
    }

    /**
     * Writes the statistics to the file.
     * @param outputStatisticsList The list of statistics to write.
     */
    public void writeStatistics(List<Statistics> outputStatisticsList) {
        File statsFile = new File(filename);
        boolean fileExists = statsFile.exists();
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename, true))) {
            if (!fileExists) {
                writeLineWithFormat(collectHeader(outputStatisticsList), writer);
            }
            writeLineWithFormat(collectStats(outputStatisticsList), writer);
        } catch (IOException e) {
            System.out.println("An error occurred while generating the file.");
            throw new RuntimeException(e);
        }
    }

    private void writeLineWithFormat(String[] row, PrintWriter writer) {
        int i = 0;
        for (String s : row) {
            if (i++ == 0) {
                writer.print(s);
                continue;
            }
            writer.print("\t");
            writer.print(s);
        }
        writer.println();
    }

    private String[] collectHeader(List<Statistics> outputStatisticsList) {
        int numberOfStats = 0;
        for (Statistics stats : outputStatisticsList) {
            numberOfStats += stats.getStatCount();
        }
        int headerStringLength = numberOfStats + 1; // +1 the timestamp
        String[] header = new String[headerStringLength];
        int headerIndex = 0;
        header[headerIndex++] = TIMESTAMP;
        headerIndex = collectTierHeader(header, headerIndex, StoreOptionType.MEMORY_ONLY, outputStatisticsList.get(0));
        headerIndex = collectTierHeader(header, headerIndex, StoreOptionType.DISK_ONLY, outputStatisticsList.get(1));
        headerIndex = collectTierHeader(header, headerIndex, StoreOptionType.S3_ONLY, outputStatisticsList.get(2));
        collectStorageHeader(header, headerIndex, outputStatisticsList.get(3),
                outputStatisticsList.get(4), outputStatisticsList.get(5));

        return header;
    }

    private int collectTierHeader(String[] header, int headerIndex, StoreOptionType tierType, Statistics stats) {
        String tierChar = "";
        if (tierType == StoreOptionType.MEMORY_ONLY) {
            tierChar = "(M)";
        } else if (tierType == StoreOptionType.DISK_ONLY) {
            tierChar = "(D)";
        } else if (tierType == StoreOptionType.S3_ONLY) {
            tierChar = "(S3)";
        }
        for (StatisticType stat : stats.getStatTypes()) {
            header[headerIndex++] = stat + tierChar;
        }
        return headerIndex;
    }

    private void collectStorageHeader(String[] header, int headerIndex, Statistics storageStats,
                                      Statistics aggregationStats, Statistics maxStats) {
        // Prepare the global stats
        collectGlobalStats(header, headerIndex, storageStats, aggregationStats, maxStats);
    }

    private void collectGlobalStats(Statistics storageStats,
                                    Statistics aggregationStats, Statistics maxStats) {
        collectGlobalStats(null, 0, storageStats, aggregationStats, maxStats);
    }

    private void collectGlobalStats(String[] header, int headerIndex, Statistics storageStats,
                                    Statistics aggregationStats, Statistics maxStats) {
        globalStats = new PerformanceStatistics();
        globalStats.clearStats();

        // GET
        collectGetStats(storageStats, aggregationStats, maxStats);
        // PUT
        collectPutStats(storageStats, aggregationStats, maxStats);
        // DELETE
        collectDeleteStats(storageStats, aggregationStats, maxStats);

        if (header != null) {
            for (StatisticType statType : globalStats.getStatTypes()) {
                header[headerIndex++] = statType.toString();
            }
        }
    }

    private void collectGetStats(Statistics storageStats,
                                 Statistics aggregationStats, Statistics maxStats) {
        collectStats(storageStats, aggregationStats, maxStats, StatisticType.listGlobalGetStats());
    }

    private void collectPutStats(Statistics storageStats,
                                 Statistics aggregationStats, Statistics maxStats) {
        collectStats(storageStats, aggregationStats, maxStats, StatisticType.listGlobalPutStats());
    }

    private void collectDeleteStats(Statistics storageStats,
                                    Statistics aggregationStats, Statistics maxStats) {
        collectStats(storageStats, aggregationStats, maxStats, StatisticType.listGlobalDelStats());
    }

    private void collectStats(Statistics storageStats,
                              Statistics aggregationStats, Statistics maxStats,
                              List<StatisticType> statTypes) {
        for (StatisticType statType : statTypes) {
            if (aggregationStats.containsStat(statType)) {
                globalStats.setStatWithValue(statType, aggregationStats.getStat(statType));
            } else if (storageStats.containsStat(statType)) {
                globalStats.setStatWithValue(statType, storageStats.getStat(statType));
            } else if (maxStats.containsStat(statType)) {
                globalStats.setStatWithValue(statType, maxStats.getStat(statType));
            } else {
                globalStats.setStatWithValue(statType, 0);
            }
        }
    }

    private String[] collectStats(List<Statistics> outputStatisticsList) {
        int numberOfStats = 0;
        for (Statistics stats : outputStatisticsList) {
            numberOfStats += stats.getStatCount();
        }
        int tierStatsStringLength = numberOfStats + 1; // +1 for the timestamp
        String[] tierStatsString = new String[tierStatsStringLength];
        tierStatsString[0] = String.valueOf(System.currentTimeMillis());
        int index = 1;
        index = collectTierStats(tierStatsString, index, outputStatisticsList.get(0));
        index = collectTierStats(tierStatsString, index, outputStatisticsList.get(1));
        index = collectTierStats(tierStatsString, index, outputStatisticsList.get(2));
        collectGlobalStats(outputStatisticsList.get(3), outputStatisticsList.get(4), outputStatisticsList.get(5));
        collectStorageStats(tierStatsString, index);
        return tierStatsString;
    }

    private int collectTierStats(String[] tierStatsString, int index, Statistics stats) {
        for (StatisticType stat : stats.getStatTypes()) {
            tierStatsString[index++] = stats.getStat(stat).toString();
        }
        return index;
    }

    private void collectStorageStats(String[] storageStatsString, int index) {
        for (StatisticType stat : globalStats.getStatTypes()) {
            if (stat.isMeasuredInDouble()) {
                storageStatsString[index++] = String.format("%.2f", globalStats.getStat(stat).doubleValue());
            } else {
                storageStatsString[index++] = globalStats.getStat(stat).toString();
            }
        }
    }

}
