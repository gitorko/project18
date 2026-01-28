package com.taskprocessor.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Configuration for the task processing thread pool.
 * Provides a dynamically resizable thread pool with monitoring.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class ThreadPoolConfig {

    private final TaskProcessorProperties properties;
    private final MeterRegistry meterRegistry;

    /**
     * Creates a ThreadPoolExecutor that can be dynamically resized at runtime.
     * The pool is monitored via Micrometer metrics.
     */
    @Bean(name = "taskProcessorExecutor")
    public ThreadPoolExecutor taskProcessorExecutor() {
        AtomicInteger threadCounter = new AtomicInteger(0);
        
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                properties.getPoller().getPoolSize(),
                properties.getPoller().getMaxPoolSize(),
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> {
                    Thread thread = new Thread(r);
                    thread.setName("task-processor-" + threadCounter.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Allow core threads to timeout when idle
        executor.allowCoreThreadTimeOut(true);

        // Register metrics for the executor
        ExecutorServiceMetrics.monitor(
                meterRegistry,
                executor,
                "taskProcessorExecutor"
        );

        log.info("Task processor thread pool initialized with core={}, max={}", 
                properties.getPoller().getPoolSize(),
                properties.getPoller().getMaxPoolSize());

        return executor;
    }
}
