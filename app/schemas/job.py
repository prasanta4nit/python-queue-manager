import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.models.job import JobStatus


# ── Inbound ────────────────────────────────────────────────────────────────────

# ── Salesforce trigger payload ─────────────────────────────────────────────────

class InternalCommand(BaseModel):
    cmd: str = Field(..., description="Module to run, e.g. importEstimate, dealReg")
    rootRecordId: Optional[str] = None
    SOQL: Optional[str] = None
    additionalParameters: Optional[Dict[str, Any]] = None


class ExternalOrchestrator(BaseModel):
    callbackURL: Optional[str] = None
    internalCommand: InternalCommand
    orchestratorUrl: Optional[str] = None
    orgId: Optional[str] = None
    priority: Optional[str] = "low"       # "low" | "high"
    sId: Optional[str] = None
    stopOnError: Optional[bool] = True
    transformClassName: Optional[str] = None


class OrchestratorRequest(BaseModel):
    additionalParameters: Optional[Dict[str, Any]] = None
    chunkNumber: Optional[int] = None
    entities: Optional[Any] = None
    initialField: Optional[str] = None
    isFirstChunk: Optional[bool] = None
    isLastChunk: Optional[bool] = None
    jobId: Optional[str] = None
    records: Optional[Any] = None
    relatedRecordIds: Optional[Any] = None
    rootRecordId: Optional[str] = None
    timeStamp: Optional[str] = None
    totalChunks: Optional[int] = None


class SalesforceJobRequest(BaseModel):
    """
    Payload sent by Salesforce to trigger a job.
    The cmd inside internalCommand determines which Python module runs.
    """
    chunkSize: int = Field(default=100, ge=1)
    externalOrchestrator: ExternalOrchestrator
    isDebug: bool = False
    operation: Optional[str] = None
    orchestratorRequest: OrchestratorRequest


# ── Generic inbound ────────────────────────────────────────────────────────────

class JobCreateRequest(BaseModel):
    """Payload sent by Salesforce (or any caller) to submit a new job."""
    customer_name: str = Field(..., min_length=1, max_length=255)
    user_name: str = Field(..., min_length=1, max_length=255)
    worker_script: str = Field(
        ...,
        description="Path to the Python worker script, relative to WORKERS_BASE_PATH"
    )
    job_type: str = Field(default="short", pattern="^(short|long)$")
    chunk_size: Optional[int] = Field(default=None, ge=1)
    parameters: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Arbitrary key-value pairs forwarded to the worker script"
    )


class JobProgressUpdate(BaseModel):
    """Payload posted by worker scripts to report incremental progress."""
    processed_records: Optional[int] = Field(default=None, ge=0)
    error_message: Optional[str] = None


# ── Outbound ───────────────────────────────────────────────────────────────────

class JobResponse(BaseModel):
    job_id: uuid.UUID
    status: JobStatus
    customer_name: str
    start_datetime: Optional[datetime]
    processed_records: int
    chunk_size: Optional[int]
    user_name: str
    error_message: Optional[str]
    retry_count: int
    job_type: str
    worker_script: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class JobListResponse(BaseModel):
    total: int
    jobs: List[JobResponse]


class WorkerStatusResponse(BaseModel):
    active_workers: int
    max_workers: int
    available_slots: int
    running_jobs: List[str]  # job_id strings
