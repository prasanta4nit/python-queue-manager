from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Application
    APP_NAME: str = "PyQM"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    API_PREFIX: str = "/api/v1"
    PORT: int = 8000

    # Database
    # Local testing  : sqlite+aiosqlite:///./pyqm.db
    # AWS RDS MySQL  : mysql+aiomysql://<user>:<password>@<host>:3306/<dbname>
    DATABASE_URL: str = "sqlite+aiosqlite:///./pyqm.db"
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20

    # AWS Configuration
    AWS_REGION: str = "us-east-1"
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None

    # SQS Configuration
    SQS_QUEUE_URL: str = "https://sqs.us-east-1.amazonaws.com/000000000000/pyqm-jobs"
    SQS_VISIBILITY_TIMEOUT: int = 300           # 5 min for short jobs
    SQS_LONG_JOB_VISIBILITY_TIMEOUT: int = 3600  # 1 hr for long jobs
    SQS_MAX_NUMBER_OF_MESSAGES: int = 10
    SQS_WAIT_TIME_SECONDS: int = 20             # Long polling
    SQS_VISIBILITY_EXTENSION_INTERVAL: int = 240  # Extend before short timeout expires

    # Worker Pool — controls how many jobs run in parallel on this EC2 instance
    MAX_CONCURRENT_WORKERS: int = 5

    # Job Retry
    MAX_RETRY_COUNT: int = 3
    RETRY_DELAY_SECONDS: int = 60  # Base delay; multiplied by retry count (linear backoff)

    # Job Timeouts (seconds)
    SHORT_JOB_TIMEOUT: int = 300    # 5 minutes
    LONG_JOB_TIMEOUT: int = 3600   # 1 hour

    # Worker Scripts Base Path
    WORKERS_BASE_PATH: str = "./workers"

    # Optional Salesforce API key for authenticating inbound requests
    SF_API_KEY: Optional[str] = None

    model_config = {"env_file": ".env", "case_sensitive": True}


@lru_cache()
def get_settings() -> Settings:
    return Settings()
