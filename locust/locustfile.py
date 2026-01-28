from locust import HttpUser, task, between
import random


class TaskApiUser(HttpUser):
    # Wait time between task executions per user
    wait_time = between(1, 3)

    @task
    def create_task(self):
        payload = {
            "taskType": "REPORT_GENERATION",
            "payload": "{\"reportId\":\"RPT-8891\"}",
            "priority": 10,
            "maxAttempts": 5
        }
        report_id = f"RPT-{random.randint(1000,9999)}"
        payload["payload"] = f'{{"reportId":"{report_id}"}}'

        headers = {
            "Content-Type": "application/json"
        }

        # Send POST request
        with self.client.post(
            "/api/v1/tasks",
            json=payload,
            headers=headers,
            name="POST /api/v1/tasks",
            catch_response=True
        ) as response:

            # Optional validation logic
            if response.status_code != 200 and response.status_code != 201:
                response.failure(f"Unexpected status: {response.status_code} - {response.text}")
            else:
                response.success()
