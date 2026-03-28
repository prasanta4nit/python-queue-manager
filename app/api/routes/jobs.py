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
    SalesforceJobRequest,
)
from app.services.cmd_registry import get_job_type, get_worker_script, list_registered_cmds
from app.services.job_service import JobService
from app.services.sqs_service import SQSService

router = APIRouter(prefix="/jobs", tags=["jobs"])

_sqs = SQSService()


# ── Salesforce trigger ─────────────────────────────────────────────────────────

@router.post("/trigger", response_model=JobResponse, status_code=201)
async def trigger_salesforce_job(
    request: SalesforceJobRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Primary entry point for Salesforce.

    Salesforce sends its standard orchestrator payload; PyQM extracts the `cmd`
    from `externalOrchestrator.internalCommand.cmd`, looks up the matching Python
    module in the CMD registry, and enqueues the job.

    The full payload is forwarded to the worker script via stdin so every worker
    has access to orchestratorRequest, callbackURL, rootRecordId, etc.
    """
    cmd = request.externalOrchestrator.internalCommand.cmd
    worker_script = get_worker_script(cmd)

    if not worker_script:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown cmd: '{cmd}'. "
                f"Registered cmds: {list_registered_cmds()}. "
                f"Add it to app/services/cmd_registry.py"
            ),
        )

    org_id     = request.externalOrchestrator.orgId or "unknown"
    priority   = request.externalOrchestrator.priority or "low"
    job_type   = get_job_type(cmd, priority)

    svc = JobService(db)
    job = await svc.create_job(
        JobCreateRequest(
            customer_name=org_id,
            user_name=request.externalOrchestrator.sId or "salesforce",
            worker_script=worker_script,
            job_type=job_type,
            chunk_size=request.chunkSize,
            parameters=request.model_dump(),   # full SF payload forwarded to worker
        )
    )

    await _sqs.send_job(
        str(job.job_id),
        {
            "job_type":      job.job_type,
            "customer_name": job.customer_name,
            "user_name":     job.user_name,
            "worker_script": job.worker_script,
            "chunk_size":    job.chunk_size,
            "cmd":           cmd,
            "parameters":    json.loads(job.parameters) if job.parameters else {},
        },
    )
    return job


# ── Generic job submission ─────────────────────────────────────────────────────

@router.post("/", response_model=JobResponse, status_code=201)
async def create_job(request: JobCreateRequest, db: AsyncSession = Depends(get_db)):
    """
    Submit a job directly (non-Salesforce callers / testing).
    Specify worker_script explicitly instead of using cmd routing.
    """
    svc = JobService(db)
    job = await svc.create_job(request)

    await _sqs.send_job(
        str(job.job_id),
        {
            "job_type":      job.job_type,
            "customer_name": job.customer_name,
            "user_name":     job.user_name,
            "worker_script": job.worker_script,
            "chunk_size":    job.chunk_size,
            "parameters":    json.loads(job.parameters) if job.parameters else {},
        },
    )
    return job


# ── Query ──────────────────────────────────────────────────────────────────────

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
    """Update job progress — called by worker scripts during execution."""
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
