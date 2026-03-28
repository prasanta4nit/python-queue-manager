"""
Worker: dealReg
cmd: dealReg

Triggered by Salesforce for deal registration processing.
The full Salesforce payload is available in config (passed via stdin).
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Any, Dict
from workers.base_worker import BaseWorker


class DealRegWorker(BaseWorker):
    def execute(self, config: Dict[str, Any]) -> int:
        # ── Extract Salesforce payload sections ────────────────────────────────
        ext_orch   = config.get("externalOrchestrator", {})
        orch_req   = config.get("orchestratorRequest", {})
        internal   = ext_orch.get("internalCommand", {})
        chunk_size = config.get("chunkSize", 100)
        is_debug   = config.get("isDebug", False)

        root_record_id = internal.get("rootRecordId")
        soql           = internal.get("SOQL")
        callback_url   = ext_orch.get("callbackURL")
        stop_on_error  = ext_orch.get("stopOnError", True)
        namespace      = orch_req.get("additionalParameters", {}).get("namespace", "")
        object_name    = orch_req.get("additionalParameters", {}).get("objectName", "")

        self.logger.info(
            "dealReg | rootRecordId=%s | object=%s | soql=%s",
            root_record_id, object_name, soql,
        )

        if is_debug:
            self.logger.info("DEBUG mode enabled — verbose logging active")

        processed = 0

        # ── TODO: implement dealReg business logic ────────────────────────────
        #
        # Typical flow:
        #   1. Query Salesforce records using rootRecordId or SOQL
        #      e.g. records = sf.query(soql or f"SELECT Id FROM {object_name} WHERE ...")
        #
        #   2. Process deal registration records in chunks of chunk_size:
        #        for chunk in chunked(records, chunk_size):
        #            process_deal_registrations(chunk, namespace=namespace)
        #            processed += len(chunk)
        #            self.report_progress(processed, f"{processed} deals processed")
        #            if stop_on_error and has_errors:
        #                raise RuntimeError("Stopping on first error")
        #
        #   3. Call back to Salesforce when done (if callbackURL is set):
        #        httpx.post(callback_url, json={"jobId": self.job_id, "status": "FINISHED"})
        #
        # ─────────────────────────────────────────────────────────────────────

        return processed


if __name__ == "__main__":
    DealRegWorker().run()
