package com.taskprocessor.integration;

import com.taskprocessor.config.TaskProcessorProperties;
import com.taskprocessor.dto.CreateTaskRequest;
import com.taskprocessor.entity.Task;
import com.taskprocessor.entity.TaskPriority;
import com.taskprocessor.entity.TaskStatus;
import com.taskprocessor.repository.TaskRepository;
import com.taskprocessor.service.TaskQueueService;
import com.taskprocessor.service.TaskRetentionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the real SQL (Liquibase migrations, SKIP LOCKED claim, UPDATE ... RETURNING,
 * guarded finalization, purge) against PostgreSQL. Skipped unless TASKPROCESSOR_IT_DB_URL is set:
 *
 * <pre>
 * TASKPROCESSOR_IT_DB_URL=jdbc:postgresql://localhost:55432/it TASKPROCESSOR_IT_DB_USER=postgres \
 * TASKPROCESSOR_IT_DB_PASSWORD=pw ./gradlew test --tests '*TaskQueueIntegrationTest'
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "TASKPROCESSOR_IT_DB_URL", matches = ".+")
@SpringBootTest(properties = {
        "task-processor.poller.enabled=false",
        "task-processor.retention.enabled=false",
        "task-processor.scheduler.poll-interval-ms=3600000",
        "task-processor.heartbeat.recovery-interval-ms=3600000",
        "task-processor.worker-id=it-worker"
})
class TaskQueueIntegrationTest {

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> System.getenv("TASKPROCESSOR_IT_DB_URL"));
        registry.add("spring.datasource.username",
                () -> Optional.ofNullable(System.getenv("TASKPROCESSOR_IT_DB_USER")).orElse("postgres"));
        registry.add("spring.datasource.password",
                () -> Optional.ofNullable(System.getenv("TASKPROCESSOR_IT_DB_PASSWORD")).orElse("postgres"));
    }

    @Autowired TaskQueueService queue;
    @Autowired TaskRepository repository;
    @Autowired TaskRetentionService retention;
    @Autowired TaskProcessorProperties properties;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach void clean() {
        jdbc.update("DELETE FROM task_idempotency");
        jdbc.update("UPDATE task_schedules SET active_task_id = NULL");
        jdbc.update("DELETE FROM tasks");
    }

    private UUID enqueue(String type, TaskPriority priority, int maxAttempts) {
        return queue.enqueueTask(CreateTaskRequest.builder().taskType(type).payload("{\"p\":1}")
                .priority(priority).maxAttempts(maxAttempts).build()).getId();
    }

    private String status(UUID id) {
        return jdbc.queryForObject("SELECT status FROM tasks WHERE id = ?", String.class, id);
    }

    private void purgeEverythingFinished() {
        boolean enabled = properties.getRetention().isEnabled();
        long hours = properties.getRetention().getRetentionHours();
        properties.getRetention().setEnabled(true);
        properties.getRetention().setRetentionHours(0);
        try {
            retention.purgeExpiredTasks();
        } finally {
            properties.getRetention().setEnabled(enabled);
            properties.getRetention().setRetentionHours(hours);
        }
    }

    @Test void claimsWithPayloadThenCompletesWithGuardedUpdates() {
        UUID id = enqueue("mail", TaskPriority.MEDIUM, 1);

        List<Task> claimed = queue.dequeueTasks(5);

        assertEquals(1, claimed.size());
        Task task = claimed.get(0);
        assertEquals(id, task.getId());
        assertEquals("{\"p\":1}", task.getPayload(), "claimed rows carry the payload");
        assertEquals(TaskStatus.PROCESSING, task.getStatus());
        assertEquals(1, task.getAttemptCount());
        assertEquals("it-worker", task.getWorkerId());
        assertNotNull(task.getStartedAt());
        assertEquals("PROCESSING", status(id));

        // a stale attempt number is rejected by the guard
        Task stale = Task.builder().id(id).taskType("mail").attemptCount(7).startedAt(task.getStartedAt()).build();
        queue.completeTask(stale);
        assertEquals("PROCESSING", status(id));

        queue.completeTask(task);
        assertEquals("COMPLETED", status(id));
        assertNotNull(jdbc.queryForObject("SELECT completed_at FROM tasks WHERE id = ?", Object.class, id));

        // completing twice is a no-op
        queue.completeTask(task);
        assertEquals("COMPLETED", status(id));
    }

    @Test void failureSchedulesRetryThenFailsPermanently() {
        UUID id = enqueue("mail", TaskPriority.MEDIUM, 2);
        Task first = queue.dequeueTasks(1).get(0);

        queue.failTask(first, "boom");

        assertEquals("SCHEDULED", status(id));
        assertEquals("boom", jdbc.queryForObject("SELECT error_message FROM tasks WHERE id = ?", String.class, id));
        assertNull(jdbc.queryForObject("SELECT worker_id FROM tasks WHERE id = ?", String.class, id));

        jdbc.update("UPDATE tasks SET status = 'PENDING', scheduled_at = now() - interval '1 second' WHERE id = ?", id);
        Task second = queue.dequeueTasks(1).get(0);
        assertEquals(2, second.getAttemptCount());
        queue.failTask(second, "boom again");
        assertEquals("FAILED", status(id));
    }

    @Test void concurrentWorkersNeverClaimTheSameTask() throws Exception {
        for (int i = 0; i < 120; i++) {
            enqueue("bulk", TaskPriority.MEDIUM, 1);
        }
        ExecutorService pool = Executors.newFixedThreadPool(6);
        List<Future<List<Task>>> futures = new ArrayList<>();
        CountDownLatch go = new CountDownLatch(1);
        for (int w = 0; w < 6; w++) {
            futures.add(pool.submit(() -> {
                go.await();
                List<Task> mine = new ArrayList<>();
                List<Task> batch;
                while (!(batch = queue.dequeueTasks(7)).isEmpty()) {
                    mine.addAll(batch);
                }
                return mine;
            }));
        }
        go.countDown();
        Set<UUID> seen = new HashSet<>();
        int total = 0;
        for (Future<List<Task>> f : futures) {
            for (Task t : f.get(60, TimeUnit.SECONDS)) {
                assertTrue(seen.add(t.getId()), "task claimed twice: " + t.getId());
                total++;
            }
        }
        pool.shutdownNow();
        assertEquals(120, total);
        assertEquals(120, jdbc.queryForObject("SELECT count(*) FROM tasks WHERE status='PROCESSING' AND attempt_count=1", Integer.class));
    }

    @Test void capacityCapsExcludeSaturatedTypesInSqlAndKeepPriorityOrder() {
        for (int i = 0; i < 30; i++) {
            enqueue("heavy", TaskPriority.HIGH, 1);
        }
        UUID fast = enqueue("fast", TaskPriority.LOW, 1);

        List<Task> claimed = queue.dequeueTasks(3, Map.of("heavy", 0));

        assertEquals(List.of(fast), claimed.stream().map(Task::getId).toList(),
                "a saturated type must not hide runnable work of other types");
        assertEquals(30, jdbc.queryForObject("SELECT count(*) FROM tasks WHERE status='PENDING'", Integer.class));

        // a partially used cap is honoured and the refetch reaches the other type
        List<Task> again = queue.dequeueTasks(4, Map.of("heavy", 1));
        assertEquals(1, again.stream().filter(t -> t.getTaskType().equals("heavy")).count());
    }

    @Test void priorityAndRoundRobinFairnessSelection() {
        for (int i = 0; i < 4; i++) {
            enqueue("a", TaskPriority.MEDIUM, 1);
        }
        enqueue("b", TaskPriority.MEDIUM, 1);
        UUID urgent = enqueue("c", TaskPriority.HIGH, 1);

        List<Task> claimed = queue.dequeueTasks(3);

        assertEquals(urgent, claimed.get(0).getId());
        assertEquals(List.of("c", "a", "b"), claimed.stream().map(Task::getTaskType).toList());
    }

    @Test void heartbeatsReleaseAndPurgeWorkAgainstRealSql() {
        UUID done = enqueue("mail", TaskPriority.MEDIUM, 1);
        UUID running = enqueue("mail", TaskPriority.MEDIUM, 1);
        UUID pending = enqueue("mail", TaskPriority.MEDIUM, 1);
        queue.enqueueTask(CreateTaskRequest.builder().taskType("mail").build(), "key-1");
        List<Task> claimed = queue.dequeueTasks(2);
        Task doneTask = claimed.stream().filter(t -> t.getId().equals(done)).findFirst().orElseThrow();
        Task runningTask = claimed.stream().filter(t -> !t.getId().equals(done)).findFirst().orElseThrow();
        queue.completeTask(doneTask);

        queue.updateHeartbeats(List.of(runningTask.getId()));
        assertEquals(1, queue.releaseTasks(List.of(runningTask.getId())));
        assertEquals("PENDING", status(runningTask.getId()));
        assertEquals(0, jdbc.queryForObject("SELECT attempt_count FROM tasks WHERE id = ?", Integer.class, runningTask.getId()),
                "a released attempt is not charged");

        purgeEverythingFinished();

        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM tasks WHERE id = ?", Integer.class, done));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tasks WHERE id = ?", Integer.class, pending));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tasks WHERE id = ?", Integer.class, running));
    }

    @Test void purgeRemovesIdempotencyRecordsAndReplayLinks() {
        UUID failed = queue.enqueueTask(CreateTaskRequest.builder().taskType("mail").maxAttempts(1).build(), "k1").getId();
        queue.failTask(queue.dequeueTasks(1).get(0), "x");
        UUID replay = queue.replayFailedTask(failed).orElseThrow().getId();
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM task_idempotency", Integer.class));

        purgeEverythingFinished();

        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM task_idempotency", Integer.class));
        assertNull(jdbc.queryForObject("SELECT replay_of_task_id FROM tasks WHERE id = ?", UUID.class, replay));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM tasks", Integer.class));
    }

    @Test void correlationIdIsStoredWithTheTask() {
        org.slf4j.MDC.put("correlationId", "req-it-1");
        try {
            UUID id = enqueue("mail", TaskPriority.MEDIUM, 1);
            assertEquals("req-it-1", jdbc.queryForObject("SELECT correlation_id FROM tasks WHERE id = ?", String.class, id));
            assertEquals("req-it-1", queue.dequeueTasks(1).get(0).getCorrelationId());
        } finally {
            org.slf4j.MDC.clear();
        }
    }
}
