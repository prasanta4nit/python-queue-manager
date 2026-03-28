"""
Example short-running worker script.

• Expected to complete in < SHORT_JOB_TIMEOUT (default 5 min).
• Reads Salesforce export parameters from the job config.
• Replace the TODO section with real business logic.

Usage (manual test):
    echo '{"customer_name": "ACME", "chunk_size": 100, "parameters": {"total_records": 300}}' \
        | JOB_ID=test-123 python example_short_worker.py
"""

import sys
import os

# Allow running directly from the workers/ directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
from typing import Any, Dict

from workers.base_worker import BaseWorker


class ShortJobWorker(BaseWorker):
    def execute(self, config: Dict[str, Any]) -> int:
        customer = config.get("customer_name", "unknown")
        chunk_size = config.get("chunk_size") or 100
        params = config.get("parameters") or {}
        total_records = params.get("total_records", chunk_size)

        self.logger.info(
            "Processing short job for '%s': %d records in chunks of %d",
            customer, total_records, chunk_size,
        )

        processed = 0
        while processed < total_records:
            batch = min(chunk_size, total_records - processed)

            # ─────────────────────────────────────────────────────────────────
            # TODO: replace with real business logic, e.g.:
            #   records = salesforce_client.query(...)
            #   write_to_rds(records)
            # ─────────────────────────────────────────────────────────────────
            time.sleep(0.05)  # simulate work

            processed += batch
            self.report_progress(processed, f"{processed}/{total_records} records done")

        return processed


if __name__ == "__main__":
    ShortJobWorker().run()
