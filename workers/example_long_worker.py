"""
Example long-running worker script.

• Expected to run for minutes to hours (up to LONG_JOB_TIMEOUT, default 1 h).
• PyQM will send regular SQS visibility-timeout heartbeats so the message
  is not re-queued while this script is still running.
• Submit this job with job_type="long".

Usage (manual test):
    echo '{"customer_name": "BigCorp", "chunk_size": 1000, "job_type": "long",
           "parameters": {"total_records": 50000}}' \
        | JOB_ID=test-456 python example_long_worker.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from typing import Any, Dict

from workers.base_worker import BaseWorker


class LongJobWorker(BaseWorker):
    def execute(self, config: Dict[str, Any]) -> int:
        customer = config.get("customer_name", "unknown")
        chunk_size = config.get("chunk_size") or 1000
        params = config.get("parameters") or {}
        total_records = params.get("total_records", 50_000)

        self.logger.info(
            "Processing long job for '%s': %d records in chunks of %d",
            customer, total_records, chunk_size,
        )

        processed = 0
        while processed < total_records:
            batch = min(chunk_size, total_records - processed)

            # ─────────────────────────────────────────────────────────────────
            # TODO: replace with real business logic, e.g.:
            #   page = salesforce_client.bulk_query(soql, offset=processed, limit=batch)
            #   transform_and_load(page, target_db_conn)
            # ─────────────────────────────────────────────────────────────────
            time.sleep(0.2)  # simulate heavier per-batch work

            processed += batch
            self.report_progress(processed, f"{processed}/{total_records} records done")

            # Optional: check a stop-file or API flag for graceful early termination
            # if should_stop():
            #     self.logger.info("Stop signal received — exiting early")
            #     break

        return processed


if __name__ == "__main__":
    LongJobWorker().run()
