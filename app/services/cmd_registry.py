"""
CMD Registry — maps Salesforce internalCommand.cmd values to worker scripts.

To add a new module:
  1. Create  workers/<your_module>_worker.py  (inherit from BaseWorker)
  2. Add an entry here:  "yourCmd": "your_module_worker.py"

The full Salesforce payload is forwarded to the worker via stdin so every
worker has access to orchestratorRequest, externalOrchestrator, etc.
"""

from typing import Optional

# cmd value (from Salesforce) → worker script filename (relative to WORKERS_BASE_PATH)
CMD_WORKER_MAP: dict[str, str] = {
    "importEstimate":   "import_estimate_worker.py",
    "dealReg":          "deal_reg_worker.py",
    # ── add new modules here ──────────────────────────────────────────────────
    # "anotherCmd":     "another_worker.py",
}

# Commands that are expected to run longer than SHORT_JOB_TIMEOUT
LONG_RUNNING_CMDS: set[str] = {
    "importEstimate",
    "dealReg",
}


def get_worker_script(cmd: str) -> Optional[str]:
    """Return the worker script filename for a given cmd, or None if not registered."""
    return CMD_WORKER_MAP.get(cmd)


def get_job_type(cmd: str, priority: str = "low") -> str:
    """
    Determine job_type based on the cmd and Salesforce priority flag.
    'high' priority jobs are always treated as 'short' (fast lane).
    """
    if priority == "high":
        return "short"
    return "long" if cmd in LONG_RUNNING_CMDS else "short"


def list_registered_cmds() -> list[str]:
    return list(CMD_WORKER_MAP.keys())
