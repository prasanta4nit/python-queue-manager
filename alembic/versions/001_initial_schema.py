"""Initial schema — job_execution_status table

Works with both SQLite (local testing) and MySQL (AWS RDS production).

Revision ID: 001
Revises:
Create Date: 2026-03-27
"""

from alembic import op
import sqlalchemy as sa

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_execution_status",

        # ── Identity ──────────────────────────────────────────────────────────
        sa.Column("job_id", sa.String(36), primary_key=True),
        sa.Column(
            "status",
            sa.Enum(
                "QUEUED", "PROCESSING", "FINISHED",
                "AWAITING_RETRY", "FAILED", "CANCELLED",
                name="jobstatus",
            ),
            nullable=False,
            server_default="QUEUED",
        ),

        # ── Business fields ───────────────────────────────────────────────────
        sa.Column("customer_name", sa.String(255), nullable=False),
        sa.Column("start_datetime", sa.DateTime(), nullable=True),
        sa.Column("processed_records", sa.Integer(), server_default="0", nullable=False),
        sa.Column("chunk_size", sa.Integer(), nullable=True),
        sa.Column("user_name", sa.String(255), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), server_default="0", nullable=False),

        # ── Execution metadata ────────────────────────────────────────────────
        sa.Column("job_type", sa.String(50), nullable=False, server_default="short"),
        sa.Column("worker_script", sa.String(500), nullable=False),
        sa.Column("parameters", sa.Text(), nullable=True),
        sa.Column("sqs_receipt_handle", sa.Text(), nullable=True),

        # ── Timestamps ────────────────────────────────────────────────────────
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
    )

    op.create_index("ix_job_status",      "job_execution_status", ["status"])
    op.create_index("ix_job_customer",    "job_execution_status", ["customer_name"])
    op.create_index("ix_job_created_at",  "job_execution_status", ["created_at"])


def downgrade() -> None:
    op.drop_table("job_execution_status")
