import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes import admin, jobs
from app.config import get_settings
from app.db.database import init_db
from app.services.worker_manager import worker_manager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)
settings = get_settings()


# ── Application lifespan ───────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=== PyQM starting up ===")

    logger.info("Initialising database schema …")
    await init_db()

    logger.info(
        "Starting WorkerManager (max_concurrent_workers=%d) …",
        settings.MAX_CONCURRENT_WORKERS,
    )
    await worker_manager.start()

    yield  # ← application runs here

    logger.info("=== PyQM shutting down ===")
    await worker_manager.stop()


# ── FastAPI app ────────────────────────────────────────────────────────────────

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description=(
        "Python Queue Manager — FastAPI-based async job execution service "
        "that orchestrates workloads from Salesforce via AWS SQS / RDS / EC2."
    ),
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Global exception handler ───────────────────────────────────────────────────

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception for %s %s", request.method, request.url)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# ── Routes ─────────────────────────────────────────────────────────────────────

app.include_router(jobs.router, prefix=settings.API_PREFIX)
app.include_router(admin.router, prefix=settings.API_PREFIX)


# ── Health check ───────────────────────────────────────────────────────────────

@app.get("/health", tags=["ops"])
async def health():
    """
    Lightweight health check used by the EC2 load balancer / Salesforce ping.
    Returns 200 as long as the service is running.
    """
    return {
        "status": "healthy",
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "workers": {
            "active": worker_manager.active_workers,
            "max": settings.MAX_CONCURRENT_WORKERS,
            "available": worker_manager.available_slots,
        },
    }
