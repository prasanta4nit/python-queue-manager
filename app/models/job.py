import enum
import uuid

from sqlalchemy import CHAR, Column, DateTime, Enum as SAEnum, Integer, String, Text
from sqlalchemy.sql import func

from app.db.database import Base


class JobStatus(str, enum.Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    FINISHED = "FINISHED"
    AWAITING_RETRY = "AWAITING_RETRY"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class JobExecution(Base):
    """
    Single source of truth for every job's lifecycle.

    State transitions:
        QUEUED → PROCESSING → FINISHED
                            → AWAITING_RETRY → PROCESSING  (up to MAX_RETRY_COUNT)
                            → FAILED
        QUEUED → CANCELLED  (admin only)
    """

    __tablename__ = "job_execution_status"

    # ── Core identity ──────────────────────────────────────────────────────────
    # MySQL has no native UUID column — store as CHAR(36) string.
    # Python generates the UUID; no DB-side default needed.
    job_id = Column(CHAR(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    status = Column(
        SAEnum(JobStatus, name="jobstatus"),
        nullable=False,
        default=JobStatus.QUEUED,
        index=True,
    )

    # ── Business fields (required by spec) ────────────────────────────────────
    customer_name = Column(String(255), nullable=False, index=True)
    start_datetime = Column(DateTime(timezone=True), nullable=True)
    processed_records = Column(Integer, default=0, nullable=False)
    chunk_size = Column(Integer, nullable=True)
    user_name = Column(String(255), nullable=False)
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0, nullable=False)

    # ── Execution metadata ─────────────────────────────────────────────────────
    job_type = Column(String(50), nullable=False, default="short")  # "short" | "long"
    worker_script = Column(String(500), nullable=False)             # relative to WORKERS_BASE_PATH
    parameters = Column(Text, nullable=True)                        # JSON-encoded dict
    sqs_receipt_handle = Column(Text, nullable=True)                # for visibility extension / delete

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
