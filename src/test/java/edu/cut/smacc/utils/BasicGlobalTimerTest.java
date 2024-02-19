package edu.cut.smacc.utils;

import org.junit.jupiter.api.Test;

public class BasicGlobalTimerTest {

    @Test
    void testStartTimer() {
        assert BasicGlobalTimer.getElapsedTime() <= System.currentTimeMillis();
        long initTime = System.currentTimeMillis();
        BasicGlobalTimer.startTimer();
        assert BasicGlobalTimer.getElapsedTime() >= 0 && BasicGlobalTimer.getElapsedTime() <= System.currentTimeMillis() - initTime;
        initTime = System.currentTimeMillis();
        BasicGlobalTimer.resetTimer();
        assert BasicGlobalTimer.getElapsedTime() >= 0 && BasicGlobalTimer.getElapsedTime() <= System.currentTimeMillis() - initTime;

        System.out.println("BasicTimerTest.testStartTimer() passed!");
    }

}
