# Task Processor

A distributed task queue implementation using Spring Boot and PostgreSQL. Similar to JobRunr but without license restrictions.

## Features

- **Distributed Queue**: Multiple pods/nodes can process tasks without duplicates using PostgreSQL's `SELECT ... FOR UPDATE SKIP LOCKED`
- **Configurable Thread Pool**: Dynamically adjust processing capacity at runtime
- **Adaptive Polling**: Reduces DB load when queue is empty
- **Heartbeat Mechanism**: Detects and recovers stuck tasks from crashed workers
- **Retry Support**: Configurable retry attempts for failed tasks
- **Priority Queue**: Higher priority tasks are processed first
- **Scheduled Tasks**: Support for delayed task execution
- **Metrics**: Spring Actuator metrics for monitoring processing rate and performance
- **REST API**: Full API for task management and configuration

## Quick Start

### Prerequisites

- Java 17+
- PostgreSQL 12+
- Gradle 8+

### Setup

1. Start PostgreSQL (using Docker Compose):

```bash
docker-compose up -d postgres
```

2. Build and run the application:

```bash
./gradlew bootRun
```

3. The application will be available at `http://localhost:8080`

## API Reference

### Task Management

#### Create a Task

```bash
POST /api/v1/tasks
Content-Type: application/json

{
  "taskType": "sample",
  "payload": "{\"key\": \"value\"}",
  "priority": 0,
  "maxAttempts": 3
}
```

#### Create Tasks in Bulk

```bash
POST /api/v1/tasks/bulk
Content-Type: application/json

{
  "tasks": [
    {"taskType": "sample", "payload": "{\"id\": 1}"},
    {"taskType": "sample", "payload": "{\"id\": 2}"}
  ]
}
```

#### Get Task by ID

```bash
GET /api/v1/tasks/{taskId}
```

#### Get Tasks (Paginated)

```bash
GET /api/v1/tasks?status=PENDING&page=0&size=20
```

#### Get Task Summary

```bash
GET /api/v1/tasks/summary
```

Response:
```json
{
  "totalTasks": 1000,
  "pendingTasks": 500,
  "processingTasks": 10,
  "completedTasks": 480,
  "failedTasks": 10,
  "processingRatePerMinute": 120.5,
  "avgProcessingTimeMs": 450.2,
  "activeWorkers": 5,
  "currentPoolSize": 10
}
```

#### Cancel a Task

```bash
DELETE /api/v1/tasks/{taskId}
```

### Poller Configuration

#### Get Current Configuration

```bash
GET /api/v1/poller/config
```

#### Update Configuration (Scale Processing)

```bash
PUT /api/v1/poller/config
Content-Type: application/json

{
  "poolSize": 20,
  "batchSize": 50,
  "pollIntervalMs": 500,
  "enabled": true
}
```

#### Get Poller Status

```bash
GET /api/v1/poller/status
```

### Metrics

Prometheus metrics available at:
```bash
GET /actuator/prometheus
```

Key metrics:
- `tasks.enqueued` - Total tasks enqueued
- `tasks.completed` - Total tasks completed
- `tasks.failed` - Total tasks failed
- `tasks.processing.active` - Currently processing tasks
- `tasks.processing.duration` - Processing time histogram
- `tasks.processing.rate` - Tasks per minute

## Configuration

### Application Properties

```yaml
task-processor:
  # Unique worker ID (auto-generated if not set)
  worker-id: ${HOSTNAME:${random.uuid}}
  
  poller:
    enabled: true
    pool-size: 5           # Initial thread pool size
    max-pool-size: 20      # Maximum threads
    batch-size: 10         # Tasks per poll
    poll-interval-ms: 1000 # Base poll interval
    adaptive-polling: true # Reduce polling when queue empty
  
  heartbeat:
    interval-ms: 5000      # Heartbeat frequency
    stale-threshold-ms: 30000  # When to consider task stuck
    recovery-interval-ms: 10000 # Stale task check frequency
```

### Environment Variables

For Kubernetes/Docker deployments:

```bash
TASK_PROCESSOR_WORKER_ID=pod-name
TASK_PROCESSOR_POLLER_POOL_SIZE=20
TASK_PROCESSOR_POLLER_BATCH_SIZE=50
SPRING_DATASOURCE_URL=jdbc:postgresql://db:5432/taskprocessor
```

## Scaling for High Throughput

When you have millions of tasks to process:

1. **Increase Thread Pool Size**:
```bash
curl -X PUT http://localhost:8080/api/v1/poller/config \
  -H "Content-Type: application/json" \
  -d '{"poolSize": 50}'
```

2. **Increase Batch Size**:
```bash
curl -X PUT http://localhost:8080/api/v1/poller/config \
  -H "Content-Type: application/json" \
  -d '{"batchSize": 100}'
```

3. **Reduce Poll Interval**:
```bash
curl -X PUT http://localhost:8080/api/v1/poller/config \
  -H "Content-Type: application/json" \
  -d '{"pollIntervalMs": 100}'
```

4. **Deploy Multiple Pods**: Each pod will independently process tasks without conflicts.

## How It Works

### Distributed Locking with SKIP LOCKED

The key to preventing duplicate processing is PostgreSQL's `SKIP LOCKED`:

```sql
SELECT * FROM tasks 
WHERE status = 'PENDING' 
ORDER BY priority DESC, created_at ASC 
LIMIT 10 
FOR UPDATE SKIP LOCKED
```

This query:
1. Locks rows that are returned
2. Skips rows already locked by other transactions
3. Ensures no two workers get the same task

### Heartbeat and Recovery

1. Workers send heartbeats every 5 seconds for tasks they're processing
2. A background job checks for tasks with stale heartbeats (>30 seconds)
3. Stale tasks are either:
   - Reset to PENDING for retry (if attempts remain)
   - Marked as FAILED (if max attempts exceeded)

### Adaptive Polling

To reduce database load:
1. When tasks are found, poll interval is minimized (100ms)
2. When queue is empty, interval doubles up to max (5000ms)
3. This reduces unnecessary DB queries during idle periods

## Creating Custom Task Handlers

```java
@Component
public class EmailTaskHandler implements TaskHandler {

    @Override
    public String getTaskType() {
        return "send-email";
    }

    @Override
    public void handle(Task task) throws Exception {
        // Parse payload
        EmailPayload payload = objectMapper.readValue(
            task.getPayload(), 
            EmailPayload.class
        );
        
        // Send email
        emailService.send(payload);
    }
}
```

Then enqueue tasks with that type:
```bash
POST /api/v1/tasks
{
  "taskType": "send-email",
  "payload": "{\"to\": \"user@example.com\", \"subject\": \"Hello\"}"
}
```

```
locust -f locustfile.py
```
