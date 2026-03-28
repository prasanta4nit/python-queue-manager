import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import JobExecution, JobStatus
from app.schemas.job import JobCreateRequest


def _sid(job_id: uuid.UUID) -> str:
    """Convert UUID to the CHAR(36) string stored in MySQL."""
    return str(job_id)


class JobService:
    def __init__(self, db: AsyncSession):
        self.db = db

    # ── Create ─────────────────────────────────────────────────────────────────

    async def create_job(self, request: JobCreateRequest) -> JobExecution:
        job = JobExecution(
            customer_name=request.customer_name,
            user_name=request.user_name,
            worker_script=request.worker_script,
            job_type=request.job_type,
            chunk_size=request.chunk_size,
            parameters=json.dumps(request.parameters) if request.parameters else None,
            status=JobStatus.QUEUED,
        )
        self.db.add(job)
        await self.db.commit()
        await self.db.refresh(job)
        return job

    # ── Read ───────────────────────────────────────────────────────────────────

    async def get_job(self, job_id: uuid.UUID) -> Optional[JobExecution]:
        result = await self.db.execute(
            select(JobExecution).where(JobExecution.job_id == _sid(job_id))
        )
        return result.scalar_one_or_none()

    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        customer_name: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[JobExecution], int]:
        base_filter = []
        if status:
            base_filter.append(JobExecution.status == status)
        if customer_name:
            base_filter.append(JobExecution.customer_name == customer_name)

        count_result = await self.db.execute(
            select(func.count(JobExecution.job_id)).where(*base_filter)
        )
        total = count_result.scalar()

        result = await self.db.execute(
            select(JobExecution)
            .where(*base_filter)
            .order_by(JobExecution.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return result.scalars().all(), total

    # ── Update ─────────────────────────────────────────────────────────────────

    async def update_status(
        self,
        job_id: uuid.UUID,
        status: JobStatus,
        error_message: Optional[str] = None,
        processed_records: Optional[int] = None,
        sqs_receipt_handle: Optional[str] = None,
    ) -> Optional[JobExecution]:
        values: dict = {"status": status, "updated_at": datetime.now(timezone.utc)}

        if status == JobStatus.PROCESSING:
            values["start_datetime"] = datetime.now(timezone.utc)
        if error_message is not None:
            values["error_message"] = error_message
        if processed_records is not None:
            values["processed_records"] = processed_records
        if sqs_receipt_handle is not None:
            values["sqs_receipt_handle"] = sqs_receipt_handle

        await self.db.execute(
            update(JobExecution).where(JobExecution.job_id == _sid(job_id)).values(**values)
        )
        await self.db.commit()
        return await self.get_job(job_id)

    async def increment_retry(self, job_id: uuid.UUID) -> Optional[JobExecution]:
        """Bump retry_count and set status to AWAITING_RETRY atomically."""
        await self.db.execute(
            update(JobExecution)
            .where(JobExecution.job_id == _sid(job_id))
            .values(
                retry_count=JobExecution.retry_count + 1,
                status=JobStatus.AWAITING_RETRY,
                updated_at=datetime.now(timezone.utc),
            )
        )
        await self.db.commit()
        return await self.get_job(job_id)

    async def update_progress(
        self,
        job_id: uuid.UUID,
        processed_records: int,
        error_message: Optional[str] = None,
    ) -> Optional[JobExecution]:
        values: dict = {
            "processed_records": processed_records,
            "updated_at": datetime.now(timezone.utc),
        }
        if error_message is not None:
            values["error_message"] = error_message

        await self.db.execute(
            update(JobExecution).where(JobExecution.job_id == _sid(job_id)).values(**values)
        )
        await self.db.commit()
        return await self.get_job(job_id)

    # ── Admin ──────────────────────────────────────────────────────────────────

    async def cancel_job(self, job_id: uuid.UUID) -> Optional[JobExecution]:
        """Cancel a QUEUED job. Returns None if the job is not in QUEUED state."""
        job = await self.get_job(job_id)
        if not job or job.status != JobStatus.QUEUED:
            return None
        return await self.update_status(job_id, JobStatus.CANCELLED)
