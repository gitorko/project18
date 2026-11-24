package com.taskprocessor.service;

import java.util.concurrent.ThreadLocalRandom;

/** Exponential retry delay with bounded random jitter. Attempt numbers start at one. */
public final class RetryDelayCalculator {
    private RetryDelayCalculator() { }

    public static long delayMillis(int attemptNumber, long initialDelayMs, long maxDelayMs,
                                   double jitterFactor) {
        if (attemptNumber < 1 || initialDelayMs < 0 || maxDelayMs < 0) {
            throw new IllegalArgumentException("Retry attempt and delay settings must be non-negative");
        }
        if (maxDelayMs == 0 || initialDelayMs == 0) {
            return 0;
        }
        double safeJitter = Math.max(0, Math.min(1, jitterFactor));
        int shift = Math.min(attemptNumber - 1, 62);
        long nominal = initialDelayMs > (Long.MAX_VALUE >> shift)
                ? maxDelayMs : Math.min(initialDelayMs << shift, maxDelayMs);
        if (safeJitter == 0) {
            return nominal;
        }
        double factor = ThreadLocalRandom.current().nextDouble(1 - safeJitter, 1 + safeJitter);
        return Math.max(0, Math.min(maxDelayMs, Math.round(nominal * factor)));
    }
}
