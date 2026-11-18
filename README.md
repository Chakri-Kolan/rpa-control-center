# RPA Control Center

Local FastAPI and SQLite work-queue service for UiPath, Power Automate Desktop, and custom robots. It provides idempotent intake, transactional leases, retries, dead-letter handling, audit history, API-token protection, structured logs, health checks, and execution metrics.

## Run

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"

export RPA_API_TOKEN="set-a-local-secret"
rpa-control-center
```

OpenAPI documentation is available at `http://127.0.0.1:8080/docs`. The public health endpoint is `/health`; all queue and metrics endpoints require `X-API-Token` when `RPA_API_TOKEN` is configured.

## Robot lifecycle

1. Enqueue using a stable business reference. Repeated submissions return the existing item.
2. Lease from `/api/v1/queues/{queue}/lease` with a robot identity.
3. Complete the leased item or report a sanitized failure.
4. Failed items return to the queue until `max_attempts`, then move to `dead`.
5. Inspect `/api/v1/metrics` and each item's audit events for QA and support.

`integrations/` contains platform-neutral request mappings for UiPath and Power Automate Desktop. The API and integration payloads are tested locally; proprietary designer imports require validation in the corresponding free desktop tooling.

## Security

Keep tokens outside source control in an environment variable or platform credential store. The service defaults to loopback, uses constant-time token comparison, rejects invalid request schemas, and records state transitions without logging payloads or credentials.

## Verify

```bash
ruff check .
ruff format --check .
pytest
python scripts/export_openapi.py
git diff --exit-code integrations/openapi.json
```
