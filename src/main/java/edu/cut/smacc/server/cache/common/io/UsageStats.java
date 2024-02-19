package edu.cut.smacc.server.cache.common.io;


import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used in order to have a way to know how many space we use in a device
 *
 * @author Dr. Herodotos Herodotou
 */
public class UsageStats {

    private AtomicLong reportedUsage; // bytes used for data
    private AtomicLong actualUsage; // total bytes used based on BB capacity
    private UsageStats parentStats; // stats for parent object, maybe null
    private long maxCapacity = 0;

    public UsageStats() {
        this.reportedUsage = new AtomicLong(0L);
        this.actualUsage = new AtomicLong(0L);
        this.parentStats = null;
    }

    public UsageStats(long maxCapacity) {
        this.reportedUsage = new AtomicLong(0L);
        this.actualUsage = new AtomicLong(0L);
        this.parentStats = null;
        this.maxCapacity = maxCapacity;
    }

    public UsageStats(UsageStats parentStats) {
        this.reportedUsage = new AtomicLong(0L);
        this.actualUsage = new AtomicLong(0L);
        this.parentStats = parentStats;
    }

    public UsageStats(UsageStats parentStats, long maxCapacity) {
        this.reportedUsage = new AtomicLong(0L);
        this.actualUsage = new AtomicLong(0L);
        this.parentStats = parentStats;
        this.maxCapacity = maxCapacity;
    }

    public long getMaxCapacity() {
        return maxCapacity;
    }

    public long getReportedUsage() {
        return this.reportedUsage.get();
    }

    public long getActualUsage() {
        return this.actualUsage.get();
    }

    public void decrement(long reported, long actual) {
        long oldValue, newValue;
        if (reported != 0) {
            do {
                oldValue = reportedUsage.get();
                newValue = oldValue - reported;
                if (newValue < 0)
                    newValue = 0;
            } while (!reportedUsage.compareAndSet(oldValue, newValue));
        }

        if (actual != 0) {
            do {
                oldValue = actualUsage.get();
                newValue = oldValue - actual;
                if (newValue < 0)
                    newValue = 0;
            } while (!actualUsage.compareAndSet(oldValue, newValue));
        }

        if (this.parentStats != null)
            this.parentStats.decrement(reported, actual);
    }

    void increment(long reported, long actual) {
        this.reportedUsage.addAndGet(reported);
        this.actualUsage.addAndGet(actual);
        if (this.parentStats != null)
            this.parentStats.increment(reported, actual);
    }

    public void increment(long actual) {
        this.reportedUsage.addAndGet(actual);
        this.actualUsage.addAndGet(actual);
        if (this.parentStats != null)
            this.parentStats.increment(actual);
    }

    @Override
    public String toString() {
        return "MemoryStats [reported=" + reportedUsage + ", actual="
                + actualUsage + ", parent="
                + ((parentStats == null) ? "null" : parentStats) + "]";
    }

    public UsageStats copy() {
        UsageStats copy = new UsageStats();
        copy.reportedUsage.set(this.reportedUsage.get());
        copy.actualUsage.set(this.actualUsage.get());
        copy.parentStats = this.parentStats;
        copy.maxCapacity = this.maxCapacity;
        return copy;
    }
}
