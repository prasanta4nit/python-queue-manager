"""
WorkerManager — heart of PyQM.

Responsibilities
────────────────
• Poll SQS in a continuous loop.
• Dispatch each message to a worker coroutine, honouring MAX_CONCURRENT_WORKERS.
• Execute the designated Python script as a subprocess (asyncio.create_subprocess_exec).
• Extend SQS visibility timeout periodically for long-running jobs.
• Handle success → FINISHED, failure → AWAITING_RETRY or FAILED lifecycle transitions.
• Support graceful shutdown (cancel pending tasks, SIGTERM subprocesses).
"""

import asyncio
import json
import logging
import os
import sys
import uuid
from typing import Dict, Optional

from app.config import get_settings
from app.db.database import AsyncSessionLocal
from app.models.job import JobStatus
from app.services.job_service import JobService
from app.services.sqs_service import SQSService

logger = logging.getLogger(__name__)
settings = get_settings()


class WorkerManager:
    def __init__(self):
        # Semaphore controls how many jobs run concurrently on this EC2 instance.
        # Adjust MAX_CONCURRENT_WORKERS in .env to tune parallelism.
        self._semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_WORKERS)
        self._sqs = SQSService()
        self._running = False
        self._poll_task: Optional[asyncio.Task] = None

        # job_id → asyncio Task (for status tracking / graceful shutdown)
        self._running_jobs: Dict[str, asyncio.Task] = {}
        # job_id → subprocess (for forceful kill on timeout / shutdown)
        self._running_procs: Dict[str, asyncio.subprocess.Process] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    @property
    def active_workers(self) -> int:
        return settings.MAX_CONCURRENT_WORKERS - self._semaphore._value

    @property
    def available_slots(self) -> int:
        return self._semaphore._value

    @property
    def max_workers(self) -> int:
        return settings.MAX_CONCURRENT_WORKERS

    def get_running_jobs(self):
        return list(self._running_jobs.keys())

    async def start(self):
        self._running = True
        self._poll_task = asyncio.create_task(self._polling_loop(), name="sqs-poller")
        logger.info(
            "WorkerManager started — max_concurrent_workers=%d",
            settings.MAX_CONCURRENT_WORKERS,
        )

    async def stop(self):
        """Graceful shutdown: stop polling, wait for running jobs, kill remaining processes."""
        self._running = False

        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass

        if self._running_jobs:
            logger.info("Waiting for %d running jobs to finish…", len(self._running_jobs))
            # Give jobs up to 30 s to finish cleanly before killing
            _, pending = await asyncio.wait(
                list(self._running_jobs.values()), timeout=30
            )
            for task in pending:
                task.cancel()

        # Kill any surviving subprocesses
        for job_id, proc in list(self._running_procs.items()):
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=5)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass

        logger.info("WorkerManager stopped")

    # ── Polling loop ───────────────────────────────────────────────────────────

    async def _polling_loop(self):
        logger.info("SQS polling loop started")
        while self._running:
            try:
                free = self.available_slots
                if free == 0:
                    # All workers busy — back off and check again shortly
                    await asyncio.sleep(2)
                    continue

                messages = await self._sqs.receive_messages(
                    max_messages=min(free, settings.SQS_MAX_NUMBER_OF_MESSAGES)
                )

                for message in messages:
                    # Dispatch without awaiting so the loop keeps polling
                    asyncio.create_task(self._dispatch(message), name="dispatch")

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Unexpected error in polling loop — retrying in 5 s")
                await asyncio.sleep(5)

        logger.info("SQS polling loop stopped")

    # ── Dispatch ───────────────────────────────────────────────────────────────

    async def _dispatch(self, message: dict):
        """Parse the SQS message and hand off to a worker coroutine."""
        receipt_handle = message["ReceiptHandle"]
        try:
            body = json.loads(message["Body"])
        except json.JSONDecodeError:
            logger.error("Malformed SQS message body: %s", message["Body"][:200])
            await self._sqs.delete_message(receipt_handle)
            return

        job_id = body.get("job_id")
        if not job_id:
            logger.error("SQS message missing job_id: %s", body)
            await self._sqs.delete_message(receipt_handle)
            return

        # Guard: skip cancelled/missing jobs before acquiring a semaphore slot
        async with AsyncSessionLocal() as db:
            job = await JobService(db).get_job(uuid.UUID(job_id))
        if not job or job.status == JobStatus.CANCELLED:
            logger.info("Skipping %s — status=%s", job_id, getattr(job, "status", "not found"))
            await self._sqs.delete_message(receipt_handle)
            return

        await self._semaphore.acquire()
        task = asyncio.create_task(
            self._run_job(job_id, body, receipt_handle),
            name=f"job-{job_id}",
        )
        self._running_jobs[job_id] = task
        task.add_done_callback(lambda t: self._on_job_done(job_id, t))

    def _on_job_done(self, job_id: str, task: asyncio.Task):
        self._running_jobs.pop(job_id, None)
        self._running_procs.pop(job_id, None)
        self._semaphore.release()
        if not task.cancelled() and task.exception():
            logger.error("Job %s task raised: %s", job_id, task.exception())

    # ── Job execution ──────────────────────────────────────────────────────────

    async def _run_job(self, job_id: str, body: dict, receipt_handle: str):
        job_type = body.get("job_type", "short")
        worker_script = body.get("worker_script", "")
        timeout = settings.LONG_JOB_TIMEOUT if job_type == "long" else settings.SHORT_JOB_TIMEOUT

        logger.info("Starting job %s (type=%s, script=%s)", job_id, job_type, worker_script)

        # Mark PROCESSING
        async with AsyncSessionLocal() as db:
            await JobService(db).update_status(
                uuid.UUID(job_id),
                JobStatus.PROCESSING,
                sqs_receipt_handle=receipt_handle,
            )

        # Keep SQS message alive for long jobs
        heartbeat: Optional[asyncio.Task] = None
        if job_type == "long":
            heartbeat = asyncio.create_task(
                self._visibility_heartbeat(receipt_handle),
                name=f"heartbeat-{job_id}",
            )

        try:
            success, error_msg, processed = await asyncio.wait_for(
                self._execute_script(job_id, body, worker_script),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            error_msg = f"Job timed out after {timeout} s"
            success, processed = False, 0
            if job_id in self._running_procs:
                try:
                    self._running_procs[job_id].kill()
                except ProcessLookupError:
                    pass
            logger.error("Job %s timed out", job_id)
        except Exception as exc:
            error_msg = str(exc)
            success, processed = False, 0
            logger.exception("Job %s raised an exception", job_id)
        finally:
            if heartbeat:
                heartbeat.cancel()

        if success:
            async with AsyncSessionLocal() as db:
                await JobService(db).update_status(
                    uuid.UUID(job_id),
                    JobStatus.FINISHED,
                    processed_records=processed,
                )
            await self._sqs.delete_message(receipt_handle)
            logger.info("Job %s FINISHED (%d records)", job_id, processed)
        else:
            await self._handle_failure(job_id, body, receipt_handle, error_msg or "Unknown error")

    # ── Script execution ───────────────────────────────────────────────────────

    async def _execute_script(
        self,
        job_id: str,
        body: dict,
        worker_script: str,
    ) -> tuple[bool, Optional[str], int]:
        """
        Run `python <worker_script>` as a subprocess.

        Contract with worker scripts
        ─────────────────────────────
        • Reads job config JSON from **stdin**.
        • Env vars: JOB_ID, JOB_BODY, PYQM_API_URL, DATABASE_URL.
        • Writes JSON-lines to **stdout**, e.g. {"processed_records": 500}.
        • Exits 0 on success, non-zero on failure.
        • Writes human-readable error text to **stderr**.
        """
        script_path = os.path.join(settings.WORKERS_BASE_PATH, worker_script)
        if not os.path.exists(script_path):
            return False, f"Worker script not found: {script_path}", 0

        env = os.environ.copy()
        env.update(
            {
                "JOB_ID": job_id,
                "JOB_BODY": json.dumps(body),
                "PYQM_API_URL": f"http://localhost:{settings.PORT}",
                "DATABASE_URL": settings.DATABASE_URL,
            }
        )

        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            script_path,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        self._running_procs[job_id] = proc

        stdout_bytes, stderr_bytes = await proc.communicate(
            input=json.dumps(body).encode()
        )

        # Parse stdout for the last {"processed_records": N} line
        processed = 0
        for line in (stdout_bytes.decode(errors="replace")).splitlines():
            try:
                data = json.loads(line)
                if "processed_records" in data:
                    processed = int(data["processed_records"])
            except (json.JSONDecodeError, ValueError, KeyError):
                pass

        if proc.returncode == 0:
            return True, None, processed
        else:
            stderr_text = stderr_bytes.decode(errors="replace").strip()
            # Truncate very long stack traces stored in the DB
            return False, stderr_text[-4000:] or "Worker exited with non-zero code", processed

    # ── SQS visibility heartbeat ───────────────────────────────────────────────

    async def _visibility_heartbeat(self, receipt_handle: str):
        """
        Periodically extend SQS message visibility so long jobs are not re-queued
        while they are still running on this instance.
        """
        interval = settings.SQS_VISIBILITY_EXTENSION_INTERVAL
        while True:
            await asyncio.sleep(interval)
            try:
                await self._sqs.extend_visibility(
                    receipt_handle,
                    settings.SQS_LONG_JOB_VISIBILITY_TIMEOUT,
                )
            except Exception:
                logger.warning("Failed to extend visibility for %.30s…", receipt_handle)

    # ── Retry / failure handling ───────────────────────────────────────────────

    async def _handle_failure(
        self,
        job_id: str,
        body: dict,
        receipt_handle: str,
        error_message: str,
    ):
        async with AsyncSessionLocal() as db:
            svc = JobService(db)
            job = await svc.get_job(uuid.UUID(job_id))

            if job and job.retry_count < settings.MAX_RETRY_COUNT:
                next_retry = job.retry_count + 1
                # Linear back-off: delay = RETRY_DELAY_SECONDS × retry_number (max 900 s for SQS)
                delay = min(settings.RETRY_DELAY_SECONDS * next_retry, 900)

                await svc.increment_retry(uuid.UUID(job_id))
                await svc.update_status(
                    uuid.UUID(job_id),
                    JobStatus.AWAITING_RETRY,
                    error_message=error_message,
                )
                # Re-enqueue with delay; delete the original message
                await self._sqs.send_job(job_id, body, delay_seconds=delay)
                await self._sqs.delete_message(receipt_handle)
                logger.warning(
                    "Job %s → AWAITING_RETRY (#%d) in %d s. Reason: %s",
                    job_id, next_retry, delay, error_message[:120],
                )
            else:
                await svc.update_status(
                    uuid.UUID(job_id),
                    JobStatus.FAILED,
                    error_message=error_message,
                )
                await self._sqs.delete_message(receipt_handle)
                logger.error("Job %s → FAILED (retries exhausted). %s", job_id, error_message[:120])


# Module-level singleton — imported by the API routes and the FastAPI lifespan
worker_manager = WorkerManager()
