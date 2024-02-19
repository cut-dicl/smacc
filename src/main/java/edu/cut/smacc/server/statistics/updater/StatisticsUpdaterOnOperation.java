package edu.cut.smacc.server.statistics.updater;

public interface StatisticsUpdaterOnOperation {

    void updateOnPut(long fileSize, double putTime);

    void updateOnGet(long fileSize, double getTime);

    void updateOnDelete(long fileSize, double deleteTime);

}
