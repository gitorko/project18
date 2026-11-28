package com.taskprocessor.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration properties for the task processor.
 * All values can be overridden via application.yml or environment variables.
 */
@Data
@Component
@ConfigurationProperties(prefix = "task-processor")
public class TaskProcessorProperties {

    /**
     * Unique identifier for this worker instance.
     * Auto-generated if not provided.
     */
    private String workerId = UUID.randomUUID().toString();

    /**
     * Poller configuration
     */
    private PollerConfig poller = new PollerConfig();

    /**
     * Heartbeat configuration for detecting stuck tasks
     */
    private HeartbeatConfig heartbeat = new HeartbeatConfig();

    private RetryConfig retry = new RetryConfig();

    private AdmissionConfig admission = new AdmissionConfig();

    private RetentionConfig retention = new RetentionConfig();

    @Data
    public static class RetentionConfig {
        /** Whether finished (COMPLETED, FAILED, CANCELLED) tasks are purged automatically. */
        private boolean enabled = true;
        /** Finished tasks last updated more than this many hours ago are purged; 0 purges them on the next run. */
        private long retentionHours = 0;
        /** How often the purge runs, in milliseconds. */
        private long purgeIntervalMs = 300000;
        /** Maximum rows removed per transaction; the purge loops until fewer are found. */
        private int batchSize = 500;
    }

    @Data
    public static class RetryConfig {
        private long initialDelayMs = 1000;
        private long maxDelayMs = 60000;
        private double jitterFactor = 0.2;
    }

    @Data
    public static class AdmissionConfig {
        /** Maximum outstanding tasks across all task types; zero disables the limit. */
        private long maxOutstandingTasks = 10000;
        /** Optional per-task-type outstanding limits, keyed by taskType. */
        private Map<String, Long> maxOutstandingByType = new HashMap<>();
    }

    @Data
    public static class PollerConfig {
        /**
         * Whether the poller is enabled
         */
        private boolean enabled = true;

        /**
         * Number of threads in the processing pool
         */
        private int poolSize = 5;

        /**
         * Maximum number of threads the pool can grow to
         */
        private int maxPoolSize = 20;

        /**
         * Number of tasks to fetch in each poll
         */
        private int batchSize = 10;

        /**
         * Interval between polls in milliseconds
         */
        private long pollIntervalMs = 1000;

        /**
         * Minimum interval between polls (for backoff)
         */
        private long minPollIntervalMs = 100;

        /**
         * Maximum interval between polls (for backoff when queue is empty)
         */
        private long maxPollIntervalMs = 5000;

        /**
         * Whether to use adaptive polling (longer intervals when queue is empty)
         */
        private boolean adaptivePolling = true;

        /**
         * How many times batchSize to over-fetch as a candidate pool for fair
         * round-robin selection by task type within a priority tier.
         */
        private int fairnessCandidateMultiplier = 5;

        /**
         * Absolute cap on the candidate pool size, regardless of batchSize/multiplier,
         * to bound query and lock-holding cost.
         */
        private int maxFairnessCandidates = 200;

        /**
         * How long, in seconds, a graceful stop or application shutdown waits for running
         * tasks to finish before they are interrupted and released back to the queue.
         */
        private int shutdownTimeoutSeconds = 30;

        /**
         * Default max execution time for a task before it's cancelled and failed,
         * used when a task doesn't specify its own timeoutSeconds.
         */
        private int defaultTaskTimeoutSeconds = 120;

        /** Per-type maximum active tasks on this worker; absent types use the pool size. */
        private Map<String, Integer> maxConcurrentByType = new HashMap<>();
    }

    @Data
    public static class HeartbeatConfig {
        /**
         * Interval between heartbeat updates in milliseconds
         */
        private long intervalMs = 5000;

        /**
         * How often running tasks are checked against their execution timeout, in
         * milliseconds. Runs independently of heartbeats so timeout precision does not
         * depend on the heartbeat interval or on database latency.
         */
        private long timeoutCheckIntervalMs = 1000;

        /**
         * Time after which a task is considered stale and can be reclaimed
         */
        private long staleThresholdMs = 30000;

        /**
         * Interval for checking and recovering stale tasks
         */
        private long recoveryIntervalMs = 10000;
    }
}
