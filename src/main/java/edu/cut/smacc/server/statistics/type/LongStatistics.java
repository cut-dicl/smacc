package edu.cut.smacc.server.statistics.type;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistics that are measured in long values.
 */
public abstract class LongStatistics extends StatisticsBase {

    private Map<StatisticType, AtomicLong> statTypeMap;

    public LongStatistics(List<StatisticType> types) {
        super();

        statTypeMap = new EnumMap<>(StatisticType.class);
        for (StatisticType type : types) {
            statTypeMap.put(type, new AtomicLong(0l));
        }

    }

    @Override
    public void setStatWithValue(StatisticType stat, Number value) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicLong(0l));
        statTypeMap.get(stat).set(value.longValue());
    }

    @Override
    public void incrementStatBy(StatisticType stat, Number value) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicLong(0l));
        statTypeMap.get(stat).addAndGet(value.longValue());
    }

    @Override
    public void decrementStatBy(StatisticType stat, Number value) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicLong(0l));
        statTypeMap.get(stat).addAndGet(-value.longValue());
    }

    @Override
    public boolean containsStat(StatisticType type) {
        return statTypeMap.containsKey(type);
    }

    @Override
    public Number getStat(StatisticType stat) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicLong(0l));
        return statTypeMap.get(stat);
    }

    @Override
    public int getStatCount() {
        return statTypeMap.size();
    }

    @Override
    public Set<StatisticType> getStatTypes() {
        return statTypeMap.keySet();
    }

    @Override
    public void clearStats() {
        statTypeMap.clear();
    }

    @Override
    public void resetStats() {
        statTypeMap.replaceAll((s, v) -> new AtomicLong(0));
    }
}
