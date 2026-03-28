"""
BaseWorker — base class for all PyQM worker scripts.

How PyQM invokes a worker
─────────────────────────
1. Spawns: python <worker_script_path>
2. Writes  job config JSON to the process's **stdin**.
3. Sets environment variables:
      JOB_ID          — UUID of the job being executed
      JOB_BODY        — same JSON as stdin (convenience duplicate)
      PYQM_API_URL    — base URL of the PyQM API (e.g. http://localhost:8000)
      DATABASE_URL    — RDS connection string (if the worker needs direct DB access)
4. Reads **stdout** for JSON-line progress updates, e.g.:
      {"processed_records": 250}
   Only lines that parse as valid JSON and contain "processed_records" are used.
5. Reads **stderr** for error messages on failure.
6. Interprets exit code:
      0   → success  → job set to FINISHED
      !=0 → failure  → job retried or set to FAILED

How to write a worker
─────────────────────
class MyWorker(BaseWorker):
    def execute(self, config: dict) -> int:
        # ... do the actual work ...
        self.report_progress(records_done)
        return total_records_done

if __name__ == "__main__":
    MyWorker().run()
"""

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import httpx

# Workers log to stderr so stdout stays clean for machine-readable JSON progress
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    stream=sys.stderr,
)


class BaseWorker(ABC):
    def __init__(self):
        self.job_id: str = os.environ.get("JOB_ID", "")
        self.api_url: str = os.environ.get("PYQM_API_URL", "http://localhost:8000")
        self.job_config: Dict[str, Any] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._processed_records: int = 0

    # ── Config loading ─────────────────────────────────────────────────────────

    def load_config(self) -> Dict[str, Any]:
        """
        Load job configuration from stdin (primary) or the JOB_BODY env var (fallback).
        Always call this before execute().
        """
        raw_stdin = sys.stdin.read().strip()
        if raw_stdin:
            try:
                self.job_config = json.loads(raw_stdin)
                return self.job_config
            except json.JSONDecodeError:
                pass

        env_body = os.environ.get("JOB_BODY", "")
        if env_body:
            try:
                self.job_config = json.loads(env_body)
            except json.JSONDecodeError:
                self.logger.warning("Could not parse JOB_BODY env var")

        return self.job_config

    # ── Progress reporting ─────────────────────────────────────────────────────

    def report_progress(self, processed_records: int, message: Optional[str] = None):
        """
        Emit a progress update.

        • Writes a JSON line to stdout (read by PyQM).
        • Also calls the PyQM API (best-effort; ignored on failure).
        """
        self._processed_records = processed_records
        payload: Dict[str, Any] = {"processed_records": processed_records}
        if message:
            payload["message"] = message

        # Machine-readable line read by WorkerManager._execute_script
        print(json.dumps(payload), flush=True)

        # Optional real-time API update (useful for monitoring dashboards)
        if self.job_id and self.api_url:
            try:
                with httpx.Client(timeout=5.0) as client:
                    client.patch(
                        f"{self.api_url}/api/v1/jobs/{self.job_id}/progress",
                        json={"processed_records": processed_records},
                    )
            except Exception:
                pass  # Never let a progress call fail the job

    # ── Main entry point ───────────────────────────────────────────────────────

    @abstractmethod
    def execute(self, config: Dict[str, Any]) -> int:
        """
        Implement job logic here.

        Parameters
        ----------
        config : dict
            The full job payload from SQS (customer_name, user_name, parameters, …).

        Returns
        -------
        int
            Total number of records processed (used to update processed_records in RDS).
        """

    def run(self):
        """
        Entry point called from `if __name__ == "__main__"` in every worker script.
        Handles config loading, error catching, and exit codes.
        """
        config = self.load_config()
        self.logger.info("Job %s starting — config keys: %s", self.job_id, list(config.keys()))

        try:
            processed = self.execute(config)
            # Emit final progress line so PyQM captures the definitive count
            print(json.dumps({"processed_records": processed or self._processed_records}), flush=True)
            self.logger.info("Job %s finished — processed %d records", self.job_id, processed)
            sys.exit(0)
        except Exception as exc:
            self.logger.exception("Job %s failed: %s", self.job_id, exc)
            # Write error context to stdout for PyQM to capture
            print(
                json.dumps(
                    {"error": str(exc), "processed_records": self._processed_records}
                ),
                flush=True,
            )
            # Human-readable stack trace goes to stderr
            sys.stderr.write(f"ERROR: {exc}\n")
            sys.exit(1)
