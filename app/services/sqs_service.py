import json
import logging
from typing import Any, Dict, List

import aioboto3

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class SQSService:
    """Thin async wrapper around AWS SQS for job queue operations."""

    def __init__(self):
        self._session = aioboto3.Session(
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION,
        )
        self.queue_url = settings.SQS_QUEUE_URL

    # ── Send ───────────────────────────────────────────────────────────────────

    async def send_job(
        self,
        job_id: str,
        job_data: Dict[str, Any],
        delay_seconds: int = 0,
    ) -> str:
        """Enqueue a job message and return the SQS MessageId."""
        body = json.dumps({"job_id": job_id, **job_data})
        async with self._session.client("sqs") as sqs:
            response = await sqs.send_message(
                QueueUrl=self.queue_url,
                MessageBody=body,
                # SQS max delay is 900 s; caller must clamp larger values
                DelaySeconds=min(delay_seconds, 900),
                MessageAttributes={
                    "job_type": {
                        "StringValue": job_data.get("job_type", "short"),
                        "DataType": "String",
                    }
                },
            )
        message_id = response["MessageId"]
        logger.info("Enqueued job %s → SQS message %s", job_id, message_id)
        return message_id

    # ── Receive ────────────────────────────────────────────────────────────────

    async def receive_messages(self, max_messages: int = None) -> List[dict]:
        """Long-poll SQS and return raw message dicts (Body + ReceiptHandle)."""
        async with self._session.client("sqs") as sqs:
            response = await sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages or settings.SQS_MAX_NUMBER_OF_MESSAGES,
                WaitTimeSeconds=settings.SQS_WAIT_TIME_SECONDS,
                VisibilityTimeout=settings.SQS_VISIBILITY_TIMEOUT,
                MessageAttributeNames=["All"],
            )
        return response.get("Messages", [])

    # ── Delete ─────────────────────────────────────────────────────────────────

    async def delete_message(self, receipt_handle: str) -> None:
        """Remove a successfully processed (or permanently failed) message from the queue."""
        async with self._session.client("sqs") as sqs:
            await sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
        logger.debug("Deleted SQS message %.30s…", receipt_handle)

    # ── Visibility ─────────────────────────────────────────────────────────────

    async def extend_visibility(self, receipt_handle: str, visibility_timeout: int) -> None:
        """Extend the visibility timeout for a long-running job's message."""
        async with self._session.client("sqs") as sqs:
            await sqs.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=visibility_timeout,
            )
        logger.debug(
            "Extended visibility to %ds for %.30s…", visibility_timeout, receipt_handle
        )
