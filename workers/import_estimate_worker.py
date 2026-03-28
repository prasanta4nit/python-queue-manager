"""
Worker: importEstimate
cmd: importEstimate

Triggered by Salesforce when it needs to import estimate data.
The full Salesforce payload is available in config (passed via stdin).
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Any, Dict

from workers.base_worker import BaseWorker
from salesforce.orchestrator_client import SalesforceOrchestratorClient
from salesforce.errors import StopOnError, StopJobIteration


class ImportEstimateWorker(BaseWorker):
    def execute(self, config: Dict[str, Any]) -> int:
        ext_orch       = config.get("externalOrchestrator", {})
        orch_req       = config.get("orchestratorRequest", {})
        internal       = ext_orch.get("internalCommand", {})
        chunk_size     = config.get("chunkSize", 100)
        is_debug       = config.get("isDebug", False)

        root_record_id = internal.get("rootRecordId")
        content_doc_id = internal.get("additionalParameters", {}).get("contentDocumentId")
        namespace      = orch_req.get("additionalParameters", {}).get("namespace", "")
        object_name    = orch_req.get("additionalParameters", {}).get("objectName", "")

        self.logger.info(
            "importEstimate | rootRecordId=%s | contentDocumentId=%s | object=%s",
            root_record_id, content_doc_id, object_name,
        )
        if is_debug:
            self.logger.info("DEBUG mode enabled")
        time.sleep(60)  # simulate initial setup work (e.g. auth, downloading file, etc.)

        # ── Step 1: Retrieve / build the records list ──────────────────────────
        # TODO: Download file from Salesforce using content_doc_id,
        #       parse it (Excel/CSV), and build list_of_records.
        #
        # Example:
        #   df = pd.read_excel(download_file(content_doc_id))
        #   list_of_records = df.to_dict("records")
        #
        list_of_records = []   # ← replace with real data

        if not list_of_records:
            self.logger.warning("No records to process for job %s", self.job_id)
            return 0

        # ── Step 2: Upsert records via the Salesforce orchestrator ────────────
        client = SalesforceOrchestratorClient(params=config, unique_id=self.job_id)

        try:
            processed = client.execute_records(
                chunk_size=chunk_size,
                list_of_ids=list_of_records,
                # template_id="your-template-id",   # pass if needed
            )
            self.report_progress(processed, f"{processed} records upserted to {object_name}")

        except StopOnError as exc:
            self.logger.error("importEstimate stopped on error: %s", exc)
            raise  # propagates → worker exits non-zero → PyQM retries / FAILED

        except StopJobIteration as exc:
            self.logger.warning("importEstimate halted by nextInChain: %s", exc)
            raise

        return processed


if __name__ == "__main__":
    ImportEstimateWorker().run()
