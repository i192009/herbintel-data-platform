import hmac
import json
import logging
import os
import threading
import uuid
from datetime import datetime

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import pipeline
from .datasets import export_dataset
from .infra import database
from .models import ExportRequest, ProcessRequest


class JSONFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception_type"] = record.exc_info[0].__name__
        return json.dumps(payload)


handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
    logger = logging.getLogger(name)
    logger.handlers = [handler]
    logger.propagate = False
app = FastAPI(title="HerbIntel processing and metadata", version="1.0.0")
admission = threading.BoundedSemaphore(2)


def authorize(authorization: str = Header(default=""), x_org_id: str = Header(default="")) -> str:
    secret = os.environ["WORKER_API_KEY"]
    if not hmac.compare_digest(authorization, "Bearer " + secret):
        raise HTTPException(401, "invalid service key")
    # Internal service identity is restricted to one configured organization.
    if x_org_id != os.getenv("WORKER_ORG", "demo"):
        raise HTTPException(403, "organization not allowed")
    return x_org_id


@app.exception_handler(LookupError)
async def missing(request, exc):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=404, content={"error": "not found"})


@app.exception_handler(ValueError)
async def invalid(request, exc):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=409, content={"error": str(exc)})


@app.middleware("http")
async def trace(request, call_next):
    response = await call_next(request)
    response.headers["X-Trace-ID"] = str(uuid.uuid4())
    return response


@app.get("/healthz")
def health():
    with database() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    values = pipeline.operational_metrics()
    pipeline.PENDING.set(sum(values["counts"].get(k, 0) for k in ("pending", "processing")))
    pipeline.COMPLETED.set(values["counts"].get("completed", 0))
    pipeline.REJECTED.set(sum(values["counts"].get(k, 0) for k in ("failed", "quarantined")))
    pipeline.DATASET_VERSIONS.set(values["dataset_versions"])
    pipeline.FRESHNESS.set(values["freshness_seconds"] if values["freshness_seconds"] is not None else -1)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/internal/pending")
def pending(org: str = Depends(authorize)):
    return pipeline.discover(org)


@app.get("/internal/recent")
def recent(org: str = Depends(authorize)):
    with database() as conn:
        return [
            str(row["id"])
            for row in conn.execute(
                "SELECT id FROM batches WHERE org_id=%s AND kind='files' AND status IN ('completed','partial') ORDER BY created_at DESC LIMIT 20",
                (org,),
            )
        ]


@app.get("/internal/history")
def history(start: datetime, end: datetime, org: str = Depends(authorize)):
    if start >= end:
        raise HTTPException(422, "start must precede end")
    with database() as conn:
        return [
            str(row["id"])
            for row in conn.execute(
                "SELECT id FROM batches WHERE org_id=%s AND kind='files' AND created_at>=%s AND created_at<%s ORDER BY created_at LIMIT 1000",
                (org, start, end),
            )
        ]


@app.post("/internal/{operation}")
def operation(operation: str, request: ProcessRequest, org: str = Depends(authorize)):
    actions = {
        "validate": pipeline.validate_manifest,
        "process": pipeline.process,
        "results": pipeline.results,
        "quality": pipeline.quality,
        "publish": pipeline.publish,
        "verify": pipeline.verify,
        "finalize": pipeline.finalize,
    }
    if operation not in actions:
        raise HTTPException(404, "operation not found")
    if not admission.acquire(blocking=False):
        raise HTTPException(503, "worker at capacity")
    try:
        if operation == "validate":
            return actions[operation](request.batch_id, org)
        return actions[operation](request.batch_id, org, request.pipeline_version)
    finally:
        admission.release()


@app.post("/datasets/export")
def export(request: ExportRequest, org: str = Depends(authorize)):
    return export_dataset(request, org)


@app.get("/datasets")
def datasets(org: str = Depends(authorize)):
    with database() as conn:
        return conn.execute(
            "SELECT id,body,created_at FROM manifests WHERE org_id=%s AND kind='ml' ORDER BY created_at DESC LIMIT 100",
            (org,),
        ).fetchall()


@app.post("/warehouse/{step}")
def warehouse(step: str, request: ProcessRequest, org: str = Depends(authorize)):
    from .warehouse import warehouse_step

    pipeline.batch_info(request.batch_id, org)
    return warehouse_step(step, request.batch_id, org, request.pipeline_version)
