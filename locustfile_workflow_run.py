"""
Locust load test equivalent of:

    hey -z 1m -c 1000 -t 120 -m POST \
      -H "Authorization: Bearer $DIFY_API_KEY" \
      -H "Content-Type: application/json" \
      -d '{"inputs": {}, "response_mode": "blocking", "user": "abc-123"}' \
      http://localhost:5001/v1/workflows/run
"""

import os

from locust import HttpUser, between, task


API_KEY = os.environ["DIFY_API_KEY"]
WORKFLOW_USER = os.getenv("WORKFLOW_USER", "abc-123")


class WorkflowRunUser(HttpUser):
    host = "http://localhost:5001"
    wait_time = between(1, 3)

    def on_start(self):
        self.client.headers.update(
            {
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
            }
        )

    @task
    def run_workflow(self):
        payload = {
            "inputs": {},
            "response_mode": "blocking",
            "user": WORKFLOW_USER,
        }

        with self.client.post(
            "/v1/workflows/run",
            json=payload,
            catch_response=True,
            name="/v1/workflows/run",
            timeout=120,
        ) as response:
            if response.status_code != 200:
                response.failure(f"{response.status_code}: {response.text[:300]}")
                return

            try:
                body = response.json()
            except ValueError:
                response.failure(f"non-json response: {response.text[:300]}")
                return

            if "error" in body or body.get("code"):
                response.failure(str(body)[:300])
                return

            response.success()
