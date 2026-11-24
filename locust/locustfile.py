"""Locust workload for task submission, queue stats, and admission backpressure.

Run from the project root, for example:
    locust -f locust/locustfile.py --host http://localhost:8080
"""

import random
import uuid

from locust import HttpUser, task


class TaskApiUser(HttpUser):
    # The delay is increased after the API signals backpressure. This gives the
    # queue time to drain before the client submits more work.
    def on_start(self):
        self.retry_after_seconds = 0

    @task(8)
    def create_task(self):
        report_id = f"RPT-{uuid.uuid4()}"
        processing_time_ms = random.choice([10, 50, 250, 1000])
        payload = {
            "taskType": random.choice(["REPORT_GENERATION", "EMAIL", "IMAGE_RESIZE"]),
            "payload": (
                f'{{"reportId":"{report_id}",'
                f'"processingTimeMs":{processing_time_ms}}}'
            ),
            "priority": random.choice(["HIGH", "MEDIUM", "LOW"]),
            "maxAttempts": 3,
        }

        with self.client.post(
            "/api/v1/tasks",
            json=payload,
            headers={"Idempotency-Key": str(uuid.uuid4())},
            name="POST /api/v1/tasks",
            catch_response=True,
        ) as response:
            if response.status_code == 201:
                response.success()
            elif response.status_code == 503:
                # Queue-capacity rejection is an expected backpressure signal.
                # Honor Retry-After before this simulated client submits again.
                try:
                    self.retry_after_seconds = max(
                        1, int(response.headers.get("Retry-After", "5"))
                    )
                except ValueError:
                    self.retry_after_seconds = 5
                response.success()
            else:
                response.failure(
                    f"Unexpected status: {response.status_code} - {response.text}"
                )

    @task(2)
    def get_queue_summary(self):
        with self.client.get(
            "/api/v1/tasks/summary",
            name="GET /api/v1/tasks/summary",
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(
                    f"Unexpected status: {response.status_code} - {response.text}"
                )

    def wait_time(self):
        normal_delay = random.uniform(1, 3)
        backpressure_delay = self.retry_after_seconds
        self.retry_after_seconds = 0
        return max(normal_delay, backpressure_delay)
