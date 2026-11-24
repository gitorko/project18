package com.taskprocessor.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryDelayCalculatorTest {
    @Test void doublesDelayPerAttemptAndHonorsMaximum() {
        assertEquals(1000, RetryDelayCalculator.delayMillis(1, 1000, 5000, 0));
        assertEquals(2000, RetryDelayCalculator.delayMillis(2, 1000, 5000, 0));
        assertEquals(5000, RetryDelayCalculator.delayMillis(5, 1000, 5000, 0));
    }

    @Test void jitterStaysWithinConfiguredBounds() {
        for (int i = 0; i < 100; i++) {
            long delay = RetryDelayCalculator.delayMillis(2, 1000, 5000, 0.2);
            assertTrue(delay >= 1600 && delay <= 2400);
        }
    }
}
