"""
Custom exceptions for Salesforce orchestrator communication.
"""


class SalesforceError(Exception):
    """Base class for all Salesforce-related errors."""


class ExecuteError(SalesforceError):
    """HTTP call to Salesforce callbackURL failed."""
    def __init__(self, message: str, operation: str = ""):
        self.operation = operation
        super().__init__(f"[{operation}] {message}" if operation else message)


class RetrieveError(SalesforceError):
    """Failed to retrieve records from Salesforce."""


class FinishError(SalesforceError):
    """Failed to send the finish signal to Salesforce."""


class StopOnError(SalesforceError):
    """
    Raised when Salesforce responds with invalidCount > 0 and stopOnError=true.
    Processing stops immediately — no further chunks are sent.
    """


class StopJobIteration(SalesforceError):
    """
    Raised when the nextInChain response signals that processing should halt
    (e.g. success=false or halt=true in the chain response).
    """
