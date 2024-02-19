package edu.cut.smacc.server.statistics.type;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * Statistics that are measured in double values.
 */
public abstract class DoubleStatistics extends StatisticsBase {

    private Map<StatisticType, AtomicDouble> statTypeMap;

    public DoubleStatistics(List<StatisticType> types) {
        super();

        statTypeMap = new EnumMap<>(StatisticType.class);
        for (StatisticType type : types) {
            statTypeMap.put(type, new AtomicDouble(0d));
        }

    }

    @Override
    public void setStatWithValue(StatisticType stat, Number value) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicDouble(0d));
        statTypeMap.get(stat).set(value.doubleValue());
    }

    @Override
    public void incrementStatBy(StatisticType stat, Number value) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicDouble(0d));
        statTypeMap.get(stat).addAndGet(value.doubleValue());
    }

    @Override
    public void decrementStatBy(StatisticType stat, Number value) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicDouble(0d));
        statTypeMap.get(stat).addAndGet(-value.doubleValue());
    }

    @Override
    public boolean containsStat(StatisticType type) {
        return statTypeMap.containsKey(type);
    }

    @Override
    public Number getStat(StatisticType stat) {
        if (!statTypeMap.containsKey(stat))
            statTypeMap.put(stat, new AtomicDouble(0d));
        return statTypeMap.get(stat);
    }

    @Override
    public Set<StatisticType> getStatTypes() {
        return statTypeMap.keySet();
    }

    @Override
    public int getStatCount() {
        return statTypeMap.size();
    }

    @Override
    public void clearStats() {
        statTypeMap.clear();
    }

    @Override
    public void resetStats() {
        statTypeMap.replaceAll((s, v) -> new AtomicDouble(0));
    }
}
