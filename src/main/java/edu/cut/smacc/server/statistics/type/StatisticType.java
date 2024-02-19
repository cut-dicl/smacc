package edu.cut.smacc.server.statistics.type;

import java.util.ArrayList;
import java.util.List;

/**
 * Type of each statistics. Provides useful methods to list groups of statistics.
 */
public enum StatisticType {
    /* Tier stats */
    CACHE_FILES,
    CACHE_BYTES,
    // S3 stats
    GET_COUNT,
    GET_BYTES,
    PUT_COUNT,
    PUT_BYTES,
    DEL_COUNT,
    DEL_BYTES,
    EVICT_COUNT,
    EVICT_BYTES,
    HIT_COUNT,
    MISS_COUNT,
    HIT_BYTES,
    MISS_BYTES,

    /* Performance stats */
    // GET
    GET_TIME,
    GET_THROUGHPUT,
    GET_IOPS,
    GET_LATENCY,
    GET_MAX_THROUGHPUT,
    GET_MAX_IOPS,

    // PUT
    PUT_TIME,
    PUT_THROUGHPUT,
    PUT_IOPS,
    PUT_LATENCY,
    PUT_MAX_THROUGHPUT,
    PUT_MAX_IOPS,

    // DEL
    DEL_TIME,
    DEL_THROUGHPUT,
    DEL_IOPS,
    DEL_LATENCY,
    DEL_MAX_THROUGHPUT,
    DEL_MAX_IOPS;


    /* List methods */
    private static List<StatisticType> listCacheStats() {
        return List.of(CACHE_FILES, CACHE_BYTES);
    }

    private static List<StatisticType> listGetS3Stats() {
        return List.of(GET_COUNT, GET_BYTES);
    }

    private static List<StatisticType> listPutS3Stats() {
        return List.of(PUT_COUNT, PUT_BYTES);
    }

    private static List<StatisticType> listDelS3Stats() {
        return List.of(DEL_COUNT, DEL_BYTES);
    }

    public static List<StatisticType> listS3Stats() {
        ArrayList<StatisticType> list = new ArrayList<>();
        list.addAll(listGetS3Stats());
        list.addAll(listPutS3Stats());
        list.addAll(listDelS3Stats());
        return list;
    }

    public static List<StatisticType> listAggregateStats() {
        return listS3Stats();
    }

    public static List<StatisticType> listTierStats() {
        ArrayList<StatisticType> list = new ArrayList<>();
        list.addAll(listCacheStats());
        list.addAll(listS3Stats());
        list.addAll(List.of(EVICT_COUNT, EVICT_BYTES, HIT_COUNT, MISS_COUNT, HIT_BYTES, MISS_BYTES));
        return list;
    }

    public static List<StatisticType> listGetPerformanceStats() {
        return List.of(GET_TIME, GET_THROUGHPUT, GET_IOPS, GET_LATENCY);
    }

    public static List<StatisticType> listPutPerformanceStats() {
        return List.of(PUT_TIME, PUT_THROUGHPUT, PUT_IOPS, PUT_LATENCY);
    }

    public static List<StatisticType> listDeletePerformanceStats() {
        return List.of(DEL_TIME, DEL_THROUGHPUT, DEL_IOPS, DEL_LATENCY);
    }

    public static List<StatisticType> listAllPerformanceStats() {
        ArrayList<StatisticType> list = new ArrayList<>();
        list.addAll(listGetPerformanceStats());
        list.addAll(listPutPerformanceStats());
        list.addAll(listDeletePerformanceStats());
        return list;
    }

    public static List<StatisticType> listGetMaxPerformanceStats() {
        return List.of(GET_MAX_THROUGHPUT, GET_MAX_IOPS);
    }

    public static List<StatisticType> listPutMaxPerformanceStats() {
        return List.of(PUT_MAX_THROUGHPUT, PUT_MAX_IOPS);
    }

    public static List<StatisticType> listDeleteMaxPerformanceStats() {
        return List.of(DEL_MAX_THROUGHPUT, DEL_MAX_IOPS);
    }

    public static List<StatisticType> listAllMaxPerformanceStats() {
        ArrayList<StatisticType> list = new ArrayList<>();
        list.addAll(listGetMaxPerformanceStats());
        list.addAll(listPutMaxPerformanceStats());
        list.addAll(listDeleteMaxPerformanceStats());
        return list;
    }

    public static List<StatisticType> listGlobalGetStats() {
        ArrayList<StatisticType> list = new ArrayList<>();
        list.addAll(listGetS3Stats());
        list.addAll(listGetPerformanceStats());
        list.addAll(listGetMaxPerformanceStats());
        return list;
    }

    public static List<StatisticType> listGlobalPutStats() {
        ArrayList<StatisticType> list = new ArrayList<>();
        list.addAll(listPutS3Stats());
        list.addAll(listPutPerformanceStats());
        list.addAll(listPutMaxPerformanceStats());
        return list;
    }

    public static List<StatisticType> listGlobalDelStats() {
        ArrayList<StatisticType> list = new ArrayList<>();
        list.addAll(listDelS3Stats());
        list.addAll(listDeletePerformanceStats());
        list.addAll(listDeleteMaxPerformanceStats());
        return list;
    }

    public boolean isMeasuredInDouble() {
        return this == GET_THROUGHPUT || this == GET_IOPS || this == GET_LATENCY ||
                this == PUT_THROUGHPUT || this == PUT_IOPS || this == PUT_LATENCY ||
                this == DEL_THROUGHPUT || this == DEL_IOPS || this == DEL_LATENCY ||
                this == GET_MAX_THROUGHPUT || this == GET_MAX_IOPS ||
                this == PUT_MAX_THROUGHPUT || this == PUT_MAX_IOPS ||
                this == DEL_MAX_THROUGHPUT || this == DEL_MAX_IOPS;
    }

}
