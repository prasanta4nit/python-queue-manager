"""
SalesforceOrchestratorClient
─────────────────────────────
Handles all communication with the Salesforce external orchestrator:

  1. Chunk records and POST each chunk to callbackURL.
  2. Parse the response and follow the nextInChain link when present.
  3. Use the nextInChain response to decide whether to continue with
     the next chunk (or halt early).
  4. Retry each execute call with progressive delays on transient errors.
  5. Monitor Salesforce API usage from response headers.

Usage (inside a worker script):
    from salesforce.orchestrator_client import SalesforceOrchestratorClient

    client = SalesforceOrchestratorClient(params=config, unique_id=job_id)
    client.execute_records(chunk_size=100, list_of_ids=records)
"""

import json
import logging
import re
import time
from math import ceil
from typing import Any, Dict, List, Optional

import requests

from salesforce.errors import ExecuteError, StopJobIteration, StopOnError

logger = logging.getLogger(__name__)

# Retry delays in seconds: first attempt is immediate (0 s)
_RETRY_DELAYS = [0, 2, 5, 10]

# Salesforce API usage warning threshold (0–1)
_API_LIMIT_WARN_THRESHOLD = 0.70


class SalesforceOrchestratorClient:
    def __init__(self, params: Dict[str, Any], unique_id: str):
        self.unique_id = unique_id
        self.params = params
        self.iteration_number = 0

        self.external_orchestrator: Dict = params.get("externalOrchestrator", {})
        self.org_id: str = self.external_orchestrator.get("orgId", "")
        self.call_back_url: str = self.external_orchestrator.get("callbackURL", "")
        self.server_url: str = self._extract_home_url(self.call_back_url)
        self.session_id: str = self.external_orchestrator.get("sId", "")
        self.session = requests.Session()

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _extract_home_url(self, url: str) -> str:
        """Return the scheme+host portion of a URL (e.g. https://org.salesforce.com)."""
        match = re.match(r"^(https://[^/]+)", url)
        return match.group(1) if match else ""

    def _get_headers(self) -> Dict[str, str]:
        return {
            "X-PrettyPrint": "1",
            "Authorization": f"OAuth {self.session_id}",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }

    def _log_extra(self, **kwargs) -> Dict[str, str]:
        """Build the standard extra dict used by every log call."""
        return {
            "job_id": str(self.unique_id),
            "job_type": "Async",
            "api": "PyQM",
            "org_id": self.org_id,
            "job_status": "PROCESSING",
            **kwargs,
        }

    # ── Retry wrapper ──────────────────────────────────────────────────────────

    def _retry_with_delay(self, fn, *args, **kwargs):
        """
        Call fn(*args, **kwargs) up to len(_RETRY_DELAYS) times.
        Each retry is preceded by a progressively longer sleep.

        Raises immediately on StopOnError (user-controlled halt).
        Re-raises the last exception if all attempts fail.
        """
        last_exc: Optional[Exception] = None
        for delay in _RETRY_DELAYS:
            try:
                if delay:
                    logger.info(
                        "Retrying %s in %d s …",
                        fn.__name__, delay,
                        extra=self._log_extra(),
                    )
                    time.sleep(delay)
                return fn(*args, **kwargs)
            except StopOnError:
                raise  # never retry a deliberate stop
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "%s failed (delay=%ds): %s",
                    fn.__name__, delay, exc,
                    extra=self._log_extra(),
                )
        raise last_exc  # all retries exhausted

    # ── Core execute ───────────────────────────────────────────────────────────

    def execute(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST a single chunk payload to the Salesforce callbackURL.

        Returns the parsed JSON response.
        Raises ExecuteError on non-200 HTTP status.
        Checks SF API limit headers on every successful call.
        """
        response = self.session.post(
            url=self.call_back_url,
            headers=self._get_headers(),
            data=json.dumps(data),
            verify=True,
            timeout=60,
        )

        self._check_sf_api_limit(response.headers)

        if response.status_code != 200:
            error_msg = f"HTTP {response.status_code} for job {self.unique_id}: {response.text}"
            logger.error(error_msg, extra=self._log_extra())
            raise ExecuteError(response.text, operation="execute")

        return response.json()

    # ── nextInChain handling ───────────────────────────────────────────────────

    def run_process(self, next_in_chain: Dict[str, Any]) -> Dict[str, Any]:
        """
        POST to the nextInChain endpoint and return the parsed response.

        The URL is taken from next_in_chain["callbackURL"] if present,
        falling back to the original callbackURL.
        """
        url = next_in_chain.get("callbackURL") or self.call_back_url

        logger.info(
            "Calling nextInChain for job %s → %s",
            self.unique_id, url,
            extra=self._log_extra(),
        )

        response = self.session.post(
            url=url,
            headers=self._get_headers(),
            data=json.dumps(next_in_chain),
            verify=True,
            timeout=60,
        )

        self._check_sf_api_limit(response.headers)

        if response.status_code != 200:
            raise ExecuteError(response.text, operation="run_process")

        return response.json()

    def _handle_next_in_chain(self, chunk_response: Dict[str, Any]) -> bool:
        """
        Inspect a chunk response for a nextInChain directive.

        Returns
        -------
        True  → continue processing the next chunk
        False → halt (nextInChain response indicated stop)

        Raises
        ------
        StopOnError      — stopOnError=true and invalidCount > 0
        StopJobIteration — nextInChain response contains halt=true or success=false
        """
        next_in_chain = chunk_response.get("nextInChain")
        if not next_in_chain:
            return True  # no chain directive — carry on

        # Check if Salesforce is asking us to stop due to validation errors
        stop_on_error = next_in_chain.get("stopOnError") in [True, "true"]
        invalid_count = chunk_response.get("invalidCount", 0)

        if stop_on_error and invalid_count:
            error_template = chunk_response.get("errorTemplate", "")
            logger.error(
                "stopOnError triggered for job %s — invalidCount=%d, template=%s",
                self.unique_id, invalid_count, error_template,
                extra=self._log_extra(),
            )
            raise StopOnError(str(error_template))

        # Call the nextInChain endpoint and get its response
        chain_response = self.run_process(next_in_chain)

        logger.info(
            "nextInChain response for job %s: %s",
            self.unique_id, json.dumps(chain_response)[:300],
            extra=self._log_extra(),
        )

        # Use the response to decide whether to continue
        if chain_response.get("halt") is True:
            raise StopJobIteration(
                f"nextInChain returned halt=true for job {self.unique_id}"
            )

        if chain_response.get("success") is False:
            raise StopJobIteration(
                f"nextInChain returned success=false for job {self.unique_id}: "
                f"{chain_response.get('message', '')}"
            )

        return True  # chain completed successfully — continue to next chunk

    # ── Main public method ─────────────────────────────────────────────────────

    def execute_records(
        self,
        chunk_size: int,
        list_of_ids: List[Dict[str, Any]],
        template_id: str = "",
    ) -> int:
        """
        Send all records to Salesforce in chunks of chunk_size.

        Each chunk is posted to callbackURL.  If the response contains a
        nextInChain directive, run_process() is called and its response
        controls whether processing continues.

        Parameters
        ----------
        chunk_size   : records per POST
        list_of_ids  : list of record dicts (must have consistent keys)
        template_id  : optional template ID forwarded in additionalParameters

        Returns
        -------
        int : total number of records successfully sent
        """
        total_records = len(list_of_ids)
        total_chunks = ceil(total_records / chunk_size)
        current_chunk = 0

        orchestrator_request: Dict = dict(self.params.get("orchestratorRequest", {}))
        content_document_id: str = (
            self.external_orchestrator
            .get("internalCommand", {})
            .get("additionalParameters", {})
            .get("contentDocumentId", "")
        )

        logger.info(
            "Job %s — %d records, chunk_size=%d → %d chunks",
            self.unique_id, total_records, chunk_size, total_chunks,
            extra=self._log_extra(
                total_records=str(total_records),
                chunk_size=str(chunk_size),
                total_chunks=str(total_chunks),
            ),
        )

        for chunk_start in range(0, total_records, chunk_size):
            current_chunk += 1
            chunk_records = list_of_ids[chunk_start: chunk_start + chunk_size]
            is_first_chunk = current_chunk == 1
            is_last_chunk = current_chunk == total_chunks

            orchestrator_request.update(
                {
                    "relatedRecordIds": None,
                    "entities": chunk_records,
                    "isFirstChunk": is_first_chunk,
                    "isLastChunk": is_last_chunk,
                    "jobId": self.unique_id,
                    "chunkNumber": current_chunk,
                    "totalChunks": total_chunks,
                    "additionalParameters": {
                        "templateId": str(template_id),
                        "contentDocumentId": content_document_id,
                    },
                }
            )

            payload = {
                "operation": self.params.get("operation"),
                "isDebug": self.params.get("isDebug"),
                "result": orchestrator_request,
                "parentJobId": "",
            }

            logger.info(
                "Job %s — sending chunk %d/%d (records %d–%d)",
                self.unique_id, current_chunk, total_chunks,
                chunk_start, chunk_start + len(chunk_records),
                extra=self._log_extra(
                    current_chunk=str(current_chunk),
                    total_chunks=str(total_chunks),
                    total_records=str(total_records),
                ),
            )

            # POST with automatic retry on transient failures
            response = self._retry_with_delay(self.execute, payload)

            # Handle nextInChain — may raise StopOnError / StopJobIteration
            should_continue = self._handle_next_in_chain(response)
            if not should_continue:
                logger.info(
                    "Job %s — processing halted after chunk %d/%d",
                    self.unique_id, current_chunk, total_chunks,
                    extra=self._log_extra(),
                )
                break

        return chunk_start + len(chunk_records)  # total records sent

    # ── SF API limit monitor ───────────────────────────────────────────────────

    @staticmethod
    def _check_sf_api_limit(headers) -> None:
        """
        Read the Sforce-Limit-Info response header and warn when usage
        exceeds _API_LIMIT_WARN_THRESHOLD (default 70 %).

        Header format: "api-usage=7383/50000"
        """
        limit_info: str = headers.get("Sforce-Limit-Info", "")
        if not limit_info:
            logger.debug("Sforce-Limit-Info header not present in response")
            return

        try:
            usage_part = limit_info.split("=")[1]          # "7383/50000"
            used, maximum = (int(x) for x in usage_part.split("/"))
            percentage = round(used / maximum, 4)

            msg = (
                f"SF API usage: {used}/{maximum} ({percentage:.1%})"
            )
            if percentage >= _API_LIMIT_WARN_THRESHOLD:
                logger.warning("⚠ %s — above %d%% threshold", msg, int(_API_LIMIT_WARN_THRESHOLD * 100))
            else:
                logger.info(msg)
        except (IndexError, ValueError, ZeroDivisionError) as exc:
            logger.warning("Could not parse Sforce-Limit-Info '%s': %s", limit_info, exc)
