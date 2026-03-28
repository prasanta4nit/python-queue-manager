"""
Admin endpoints — intended for internal tooling / Salesforce admin users only.

In production, protect these routes with an API gateway, IAM policy, or a
middleware that checks the SF_API_KEY header before routing here.
"""

import json
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.models.job import JobExecution, JobStatus
from app.schemas.job import JobResponse, WorkerStatusResponse
from app.services.job_service import JobService
from app.services.sqs_service import SQSService
from app.services.worker_manager import worker_manager

router = APIRouter(prefix="/admin", tags=["admin"])


# ── Worker status ──────────────────────────────────────────────────────────────

@router.get("/workers/status", response_model=WorkerStatusResponse)
async def get_worker_status():
    """
    Return the current state of the worker pool on this EC2 instance.

    active_workers  — jobs currently executing
    max_workers     — configured MAX_CONCURRENT_WORKERS
    available_slots — free slots (max − active)
    running_jobs    — list of job_id strings currently in flight
    """
    return WorkerStatusResponse(
        active_workers=worker_manager.active_workers,
        max_workers=worker_manager.max_workers,
        available_slots=worker_manager.available_slots,
        running_jobs=worker_manager.get_running_jobs(),
    )


# ── Cancel ─────────────────────────────────────────────────────────────────────

@router.post("/jobs/{job_id}/cancel", response_model=JobResponse)
async def cancel_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """
    Cancel a QUEUED job (admin only).

    Only jobs in QUEUED state can be cancelled.  Jobs that are already
    PROCESSING cannot be interrupted via this endpoint.
    """
    svc = JobService(db)
    job = await svc.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.QUEUED:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel job in '{job.status}' state — only QUEUED jobs can be cancelled.",
        )

    cancelled = await svc.cancel_job(job_id)
    return cancelled


# ── Force retry ────────────────────────────────────────────────────────────────

@router.post("/jobs/{job_id}/retry", response_model=JobResponse)
async def force_retry(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """
    Force-retry a FAILED or AWAITING_RETRY job, resetting the retry counter.

    Useful when a transient infrastructure issue caused repeated failures.
    """
    svc = JobService(db)
    job = await svc.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status not in (JobStatus.FAILED, JobStatus.AWAITING_RETRY):
        raise HTTPException(
            status_code=409,
            detail=f"Can only retry FAILED or AWAITING_RETRY jobs. Current status: {job.status}",
        )

    # Reset to QUEUED and clear retry state
    await db.execute(
        update(JobExecution)
        .where(JobExecution.job_id == job_id)
        .values(status=JobStatus.QUEUED, retry_count=0, error_message=None)
    )
    await db.commit()

    sqs = SQSService()
    await sqs.send_job(
        str(job.job_id),
        {
            "job_type": job.job_type,
            "customer_name": job.customer_name,
            "user_name": job.user_name,
            "worker_script": job.worker_script,
            "chunk_size": job.chunk_size,
            "parameters": json.loads(job.parameters) if job.parameters else {},
        },
    )

    return await svc.get_job(job_id)
