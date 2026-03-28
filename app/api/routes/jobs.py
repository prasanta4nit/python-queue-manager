import json
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.database import get_db
from app.models.job import JobStatus
from app.schemas.job import (
    JobCreateRequest,
    JobListResponse,
    JobProgressUpdate,
    JobResponse,
)
from app.services.job_service import JobService
from app.services.sqs_service import SQSService

router = APIRouter(prefix="/jobs", tags=["jobs"])

_sqs = SQSService()


@router.post("/", response_model=JobResponse, status_code=201)
async def create_job(request: JobCreateRequest, db: AsyncSession = Depends(get_db)):
    """
    Submit a new job.

    Stores the job record in RDS (status=QUEUED) then pushes a message to SQS.
    The WorkerManager picks it up and executes the worker script on this EC2 instance.
    """
    svc = JobService(db)
    job = await svc.create_job(request)

    await _sqs.send_job(
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
    return job


@router.get("/", response_model=JobListResponse)
async def list_jobs(
    status: Optional[JobStatus] = Query(default=None, description="Filter by job status"),
    customer_name: Optional[str] = Query(default=None, description="Filter by customer"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List jobs with optional filtering and pagination."""
    svc = JobService(db)
    jobs, total = await svc.list_jobs(
        status=status,
        customer_name=customer_name,
        limit=limit,
        offset=offset,
    )
    return JobListResponse(total=total, jobs=jobs)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Get a single job's current status."""
    svc = JobService(db)
    job = await svc.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.patch("/{job_id}/progress", response_model=JobResponse)
async def update_job_progress(
    job_id: uuid.UUID,
    update: JobProgressUpdate,
    db: AsyncSession = Depends(get_db),
):
    """
    Update job progress.

    Called by worker scripts during execution to report processed_records count.
    Workers should POST here periodically for long-running jobs.
    """
    svc = JobService(db)
    job = await svc.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    updated = await svc.update_progress(
        job_id,
        processed_records=update.processed_records if update.processed_records is not None else job.processed_records,
        error_message=update.error_message,
    )
    return updated
