# PyQM — Python Queue Manager

PyQM is an async job execution service built on FastAPI. Salesforce (or any HTTP client) submits a job. PyQM records it in a database, puts it on an AWS SQS queue, and a pool of background workers picks it up and runs the matching Python worker script as a subprocess. Job state, progress, retries, and failures are all tracked in the `job_execution_status` table and exposed through a REST API.

Target deployment: EC2 for compute, SQS for the queue, and RDS MySQL for persistence. For local development, SQLite works with no extra setup.

---

## Contents

- [How it works](#how-it-works)
- [Job lifecycle](#job-lifecycle)
- [Project layout](#project-layout)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [API reference](#api-reference)
- [Writing a worker](#writing-a-worker)
- [Database and migrations](#database-and-migrations)
- [Testing](#testing)
- [Deploying to production](#deploying-to-production)
- [Security notes](#security-notes)
- [Further documentation](#further-documentation)

---

## How it works

```
 Salesforce / client
        │  POST /api/v1/jobs/trigger   (or POST /api/v1/jobs/)
        ▼
 ┌──────────────────┐   insert row (QUEUED)   ┌──────────────────────┐
 │  FastAPI routes  │ ──────────────────────▶ │  DB (SQLite / MySQL) │
 └──────────────────┘                         └──────────────────────┘
        │ send message                                   ▲
        ▼                                                │ status / progress
 ┌──────────────────┐   long-poll    ┌─────────────────────────────────┐
 │    AWS SQS       │ ◀───────────── │ WorkerManager (asyncio)         │
 └──────────────────┘                │  • semaphore = MAX_CONCURRENT_  │
                                     │    WORKERS                      │
                                     │  • spawns `python <worker>.py`  │
                                     │  • heartbeat for long jobs      │
                                     │  • retry / fail handling        │
                                     └─────────────────────────────────┘
                                                     │ stdin: job JSON
                                                     ▼
                                     ┌─────────────────────────────────┐
                                     │ workers/*.py (BaseWorker)       │
                                     │  → optionally calls Salesforce  │
                                     │    via SalesforceOrchestrator-  │
                                     │    Client                       │
                                     └─────────────────────────────────┘
```

1. **Submit.** `POST /api/v1/jobs/trigger` accepts the standard Salesforce orchestrator payload. PyQM reads `externalOrchestrator.internalCommand.cmd` and looks it up in the CMD registry ([app/services/cmd_registry.py](app/services/cmd_registry.py)) to find the worker script. Then it creates a `QUEUED` row and sends a message to SQS.
2. **Poll.** When the app starts, `WorkerManager` ([app/services/worker_manager.py](app/services/worker_manager.py)) begins long-polling SQS. It only requests as many messages as it has free worker slots.
3. **Execute.** For each message, the manager marks the job `PROCESSING` and runs the worker script as a subprocess. The job payload goes to the script's stdin, and the manager reads progress from its stdout.
4. **Complete.** If the script exits with code `0`, the job is marked `FINISHED` and the SQS message is deleted. If it exits non-zero or times out, the job is retried with linear backoff until `MAX_RETRY_COUNT` is reached, after which it is marked `FAILED`.

### Short vs. long jobs

| | `short` | `long` |
|---|---|---|
| Timeout | `SHORT_JOB_TIMEOUT` (300 s) | `LONG_JOB_TIMEOUT` (3600 s) |
| SQS visibility heartbeat | No | Yes. Every `SQS_VISIBILITY_EXTENSION_INTERVAL` (240 s), extended to `SQS_LONG_JOB_VISIBILITY_TIMEOUT` |

For `/trigger` requests, the job type is decided by `get_job_type()`. Commands listed in `LONG_RUNNING_CMDS` run as `long`. A request with `priority: "high"` always runs as `short`.

---

## Job lifecycle

```
QUEUED ──▶ PROCESSING ──▶ FINISHED
   │            │
   │            ├──▶ AWAITING_RETRY ──▶ PROCESSING   (up to MAX_RETRY_COUNT)
   │            │
   │            └──▶ FAILED
   │
   └──▶ CANCELLED   (admin, QUEUED jobs only)
```

- **Retry delay** is `RETRY_DELAY_SECONDS × attempt_number`, capped at 900 s, which is the maximum delay SQS allows.
- **Force retry** (`POST /admin/jobs/{id}/retry`) resets `retry_count` to 0 and re-queues a `FAILED` or `AWAITING_RETRY` job.
- **Cancelled** jobs are skipped when their SQS message arrives, and the message is deleted.
- **Graceful shutdown** stops polling, gives running jobs up to 30 s to finish, then terminates any subprocesses still running.

---

## Project layout

```
pyqm_code/
├── app/
│   ├── main.py                  # FastAPI app, lifespan (DB init + WorkerManager), /health
│   ├── config.py                # Pydantic Settings loaded from env / .env
│   ├── api/routes/
│   │   ├── jobs.py              # /jobs — trigger, submit, list, get, progress
│   │   └── admin.py             # /admin — worker status, cancel, force-retry
│   ├── db/database.py           # Async SQLAlchemy engine, session, init_db()
│   ├── models/job.py            # JobExecution ORM model + JobStatus enum
│   ├── schemas/job.py           # Pydantic request/response models (incl. Salesforce payload)
│   └── services/
│       ├── cmd_registry.py      # Salesforce cmd → worker script mapping
│       ├── job_service.py       # DB operations on JobExecution
│       ├── sqs_service.py       # aioboto3 SQS wrapper
│       └── worker_manager.py    # Polling loop, concurrency, subprocess execution, retries
├── workers/
│   ├── base_worker.py           # BaseWorker — subclass this for every worker
│   ├── import_estimate_worker.py   # cmd: importEstimate
│   ├── deal_reg_worker.py          # cmd: dealReg
│   ├── excel_sf_upsert_worker.py   # Excel → Salesforce upsert (direct submission)
│   ├── example_short_worker.py
│   └── example_long_worker.py
├── salesforce/
│   ├── orchestrator_client.py   # Chunked POSTs to callbackURL, nextInChain, retries, API-limit checks
│   └── errors.py                # StopOnError, StopJobIteration, ExecuteError, …
├── alembic/                     # Migrations (001_initial_schema)
├── docs/PYQM_CLASS_AND_METHOD_REFERENCE.md
├── PyQM.postman_collection.json
├── requirements.txt
└── .env.example
```

---

## Getting started

### Prerequisites

- Python 3.10 or newer (developed on 3.12)
- An AWS SQS queue, plus credentials that can send, receive, delete, and change visibility on it. PyQM has no local queue emulator, so even a local run needs a real queue.
- Optional: an RDS MySQL instance. Without one, SQLite is used.

### Install

```bash
python -m venv .venv

# Windows (PowerShell)
.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
pip install requests   # used by salesforce/orchestrator_client.py, not yet in requirements.txt
```

### Configure

```bash
cp .env.example .env
```

At minimum, set `SQS_QUEUE_URL`, `AWS_REGION`, and your AWS credentials. Leave `DATABASE_URL` set to SQLite for local work. All settings are listed under [Configuration](#configuration).

### Run

```bash
uvicorn app.main:app --reload --port 8000
```

On startup, PyQM creates the database tables if they don't exist and starts the SQS poller. Once it is running:

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
- Health: http://localhost:8000/health

---

## Configuration

Settings come from environment variables, or from a `.env` file in the project root. Names are case-sensitive. See [app/config.py](app/config.py).

| Variable | Default | Description |
|---|---|---|
| `APP_NAME` / `APP_VERSION` | `PyQM` / `1.0.0` | Shown in OpenAPI and `/health` |
| `DEBUG` | `false` | Turns on SQLAlchemy SQL echo |
| `API_PREFIX` | `/api/v1` | Prefix for all job and admin routes |
| `PORT` | `8000` | Used to build `PYQM_API_URL` for workers. It should match the port uvicorn listens on. |
| `DATABASE_URL` | `sqlite+aiosqlite:///./pyqm.db` | Use `mysql+aiomysql://user:pass@host:3306/db` for RDS |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | `10` / `20` | Connection pool size. Ignored for SQLite. |
| `AWS_REGION` | `us-east-1` | |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | — | Leave blank on EC2 and use an instance IAM role instead |
| `SQS_QUEUE_URL` | — | Full URL of the jobs queue |
| `SQS_VISIBILITY_TIMEOUT` | `300` | Visibility timeout applied when a message is received |
| `SQS_LONG_JOB_VISIBILITY_TIMEOUT` | `3600` | New visibility timeout set by each heartbeat on a long job |
| `SQS_VISIBILITY_EXTENSION_INTERVAL` | `240` | Seconds between heartbeats. Keep it below `SQS_VISIBILITY_TIMEOUT`. |
| `SQS_MAX_NUMBER_OF_MESSAGES` | `10` | Maximum messages per receive call (SQS limit is 10) |
| `SQS_WAIT_TIME_SECONDS` | `20` | Long-poll wait time |
| `MAX_CONCURRENT_WORKERS` | `5` | Number of jobs that can run in parallel on one instance |
| `MAX_RETRY_COUNT` | `3` | Retries allowed before a job is marked `FAILED` |
| `RETRY_DELAY_SECONDS` | `60` | Base delay for linear backoff |
| `SHORT_JOB_TIMEOUT` / `LONG_JOB_TIMEOUT` | `300` / `3600` | Subprocess timeout for each job type, in seconds |
| `WORKERS_BASE_PATH` | `./workers` | Directory that `worker_script` paths are resolved against |
| `SF_API_KEY` | — | Reserved for authenticating inbound requests. Not enforced yet; see [Security notes](#security-notes). |

---

## API reference

All routes except `/health` sit under `API_PREFIX` (default `/api/v1`). The full schema is at `/docs`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service health and worker-pool usage |
| `POST` | `/api/v1/jobs/trigger` | **Salesforce entry point.** Routes by `cmd` through the CMD registry. |
| `POST` | `/api/v1/jobs/` | Submit a job with an explicit `worker_script` (testing, non-Salesforce callers) |
| `GET` | `/api/v1/jobs/` | List jobs. Query params: `status`, `customer_name`, `limit` (1–500), `offset` |
| `GET` | `/api/v1/jobs/{job_id}` | Get one job |
| `PATCH` | `/api/v1/jobs/{job_id}/progress` | Update `processed_records` / `error_message`. Workers call this. |
| `GET` | `/api/v1/admin/workers/status` | Active workers, free slots, running job IDs |
| `POST` | `/api/v1/admin/jobs/{job_id}/cancel` | Cancel a job. Only works on `QUEUED` jobs; otherwise returns `409`. |
| `POST` | `/api/v1/admin/jobs/{job_id}/retry` | Force-retry a `FAILED` or `AWAITING_RETRY` job; otherwise returns `409` |

### Salesforce trigger

```http
POST /api/v1/jobs/trigger
Content-Type: application/json

{
  "chunkSize": 100,
  "isDebug": false,
  "externalOrchestrator": {
    "callbackURL": "https://yourorg.my.salesforce.com/services/apexrest/...",
    "orgId": "00D000000000000",
    "priority": "low",
    "sId": "<salesforce session id>",
    "stopOnError": true,
    "internalCommand": {
      "cmd": "importEstimate",
      "rootRecordId": "a0X000000000000",
      "additionalParameters": { "contentDocumentId": "069000000000000" }
    }
  },
  "orchestratorRequest": {
    "additionalParameters": { "namespace": "", "objectName": "Estimate__c" }
  }
}
```

- `orgId` is stored as `customer_name`.
- The full payload is stored as the job's `parameters` and passed to the worker unchanged.
- An unknown `cmd` returns `400` together with the list of registered commands.

### Direct submission

```http
POST /api/v1/jobs/
Content-Type: application/json

{
  "customer_name": "ACME Corp",
  "user_name": "jdoe",
  "worker_script": "example_short_worker.py",
  "job_type": "short",
  "chunk_size": 100,
  "parameters": { "total_records": 300 }
}
```

`job_type` must be `short` or `long`. `worker_script` is a path relative to `WORKERS_BASE_PATH`.

---

## Writing a worker

A worker is a standalone Python script that subclasses `BaseWorker` ([workers/base_worker.py](workers/base_worker.py)):

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Any, Dict
from workers.base_worker import BaseWorker


class MyWorker(BaseWorker):
    def execute(self, config: Dict[str, Any]) -> int:
        records = ...                      # do the work
        self.report_progress(len(records))  # optional, can be called repeatedly
        return len(records)                 # total processed


if __name__ == "__main__":
    MyWorker().run()
```

### Contract between PyQM and a worker

| Channel | Purpose |
|---|---|
| **stdin** | Job payload as JSON. `load_config()` reads it, falling back to the `JOB_BODY` env var. |
| **env** | `JOB_ID`, `JOB_BODY`, `PYQM_API_URL`, `DATABASE_URL` |
| **stdout** | JSON lines. PyQM records the last `{"processed_records": N}` line it sees. Logs must not go to stdout. |
| **stderr** | Logs and error text. On failure, the last 4000 characters are saved as `error_message`. |
| **exit code** | `0` → `FINISHED`. Anything else → retry or `FAILED`. |

`BaseWorker.run()` handles all of this: it reads the config, sends logging to stderr, catches exceptions, and sets the exit code.

### Registering a Salesforce command

1. Create `workers/<name>_worker.py` as shown above.
2. Map the command name to the script in `CMD_WORKER_MAP` in [app/services/cmd_registry.py](app/services/cmd_registry.py).
3. If the job can run longer than `SHORT_JOB_TIMEOUT`, add the command name to `LONG_RUNNING_CMDS`.

### Talking back to Salesforce

Workers that push records to Salesforce should use `SalesforceOrchestratorClient` ([salesforce/orchestrator_client.py](salesforce/orchestrator_client.py)):

```python
from salesforce.orchestrator_client import SalesforceOrchestratorClient
from salesforce.errors import StopOnError, StopJobIteration

client = SalesforceOrchestratorClient(params=config, unique_id=self.job_id)
processed = client.execute_records(chunk_size=chunk_size, list_of_ids=records)
```

The client:

- Splits records into chunks and POSTs each chunk to `callbackURL`, authenticating with the session ID from `sId`.
- Retries each POST after 0, 2, 5, and 10 seconds.
- Follows `nextInChain` directives.
- Logs a warning when `Sforce-Limit-Info` shows more than 70% of the API limit used.

It raises `StopOnError` when `stopOnError` is true and a chunk comes back with `invalidCount > 0`. It raises `StopJobIteration` when a chain response contains `halt=true` or `success=false`. If the worker re-raises either one, the job fails.

---

## Database and migrations

The app has a single table, `job_execution_status` ([app/models/job.py](app/models/job.py)). `job_id` is a `CHAR(36)` UUID because MySQL has no native UUID type.

- **Local / SQLite.** `init_db()` creates the tables on startup. Nothing else is needed.
- **Production / MySQL.** Use Alembic. It reads `DATABASE_URL` from the app settings, so the URL in `alembic.ini` is ignored.

```bash
alembic upgrade head                               # apply migrations
alembic revision --autogenerate -m "describe change"
alembic downgrade -1
```

---

## Testing

### Postman

Import [PyQM.postman_collection.json](PyQM.postman_collection.json). It has requests for health, job submission (short, long, and Excel upsert), job queries, progress updates, and the admin endpoints. Set the `base_url` and `job_id` collection variables before running them. The collection doesn't include `/jobs/trigger` yet; use the example payload [above](#salesforce-trigger) for that.

### Running a worker by hand

Workers can run without PyQM or SQS, which is the quickest way to debug one:

```bash
# bash
echo '{"customer_name":"ACME","chunk_size":100,"parameters":{"total_records":300}}' \
  | JOB_ID=test-123 python workers/example_short_worker.py
```

```powershell
# PowerShell
$env:JOB_ID = "test-123"
'{"customer_name":"ACME","chunk_size":100,"parameters":{"total_records":300}}' | python workers/example_short_worker.py
```

The worker will also try to send progress to `PYQM_API_URL`. If the API isn't running, those calls fail quietly and the worker carries on.

---

## Deploying to production

1. Set `DATABASE_URL` to the RDS MySQL instance and run `alembic upgrade head`.
2. Attach an IAM role to the EC2 instance that grants `sqs:SendMessage`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and `sqs:ChangeMessageVisibility` on the queue. Leave the `AWS_*` key variables blank.
3. Tune `MAX_CONCURRENT_WORKERS` to the instance size. Every running job is a separate Python process.
4. Run uvicorn with **one worker process per instance**, for example `uvicorn app.main:app --host 0.0.0.0 --port 8000`. Each uvicorn process starts its own `WorkerManager`, so `--workers N` would give you N pollers and N× the concurrency. To scale out, add EC2 instances instead.
5. Point the load balancer health check at `/health`.

---

## Security notes

- **No authentication on any route yet, including `/admin/*`.** `SF_API_KEY` is defined in settings but nothing checks it. Until an auth middleware is added, put the service behind an API gateway, security group, or private network.
- CORS currently allows all origins.
- The full trigger payload, including `sId` (the Salesforce session ID), is saved in the database. `sId` is also copied into `user_name`, which job API responses return. Restrict access to the database and the job endpoints accordingly.
- Never commit `.env`; it is already in `.gitignore`.

---

## Further documentation

- [docs/PYQM_CLASS_AND_METHOD_REFERENCE.md](docs/PYQM_CLASS_AND_METHOD_REFERENCE.md) is a class-by-class and method-by-method reference for `app/`, `salesforce/`, and `workers/`.
- The interactive API docs are served at `/docs` while the app is running.
