package edu.cut.smacc.utils;

public class BasicGlobalTimer {

    private static long startTime;

    public static void startTimer() {
        startTime = System.currentTimeMillis();
    }

    public static long getElapsedTime() {
        return System.currentTimeMillis() - startTime;
    }

    public static void resetTimer() {
        startTimer();
    }

}
