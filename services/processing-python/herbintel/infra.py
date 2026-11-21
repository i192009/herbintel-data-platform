import hashlib
import json
import os
import threading
from contextlib import contextmanager
from functools import lru_cache
from typing import Any

import boto3
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential

BUCKET = os.getenv("S3_BUCKET", "herbintel-data")
_pool: Any = None
_pool_lock = threading.Lock()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


@contextmanager
def database():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                import atexit

                from psycopg.rows import dict_row
                from psycopg_pool import ConnectionPool

                _pool = ConnectionPool(
                    os.environ["DATABASE_URL"],
                    min_size=1,
                    max_size=16,
                    open=True,
                    timeout=20,
                    kwargs={"row_factory": dict_row, "connect_timeout": 10},
                    check=ConnectionPool.check_connection,
                )
                atexit.register(_pool.close)
    with _pool.connection() as conn:
        conn.execute("SET LOCAL statement_timeout = '60s'")
        yield conn


def jsonb(value):
    from psycopg.types.json import Jsonb

    return Jsonb(value)


@lru_cache(maxsize=1)
def client():
    return boto3.session.Session().client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY") or None,
        aws_secret_access_key=os.getenv("S3_SECRET_KEY") or None,
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3},
            connect_timeout=5,
            read_timeout=30,
            max_pool_connections=32,
        ),
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=4), reraise=True)
def put(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    sha = digest(data)
    s3 = client()
    s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=content_type, Metadata={"sha256": sha})
    # Verify stored bytes, including implementations without S3 checksum headers.
    if digest(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()) != sha:
        raise ValueError("object checksum verification failed")
    return sha


def get(key: str) -> bytes:
    return client().get_object(Bucket=BUCKET, Key=key)["Body"].read()


def register_manifest(body: dict, org: str, batch: str, kind: str) -> dict:
    from psycopg.types.json import Jsonb

    manifest_id = digest(canonical(body))
    key = f"manifests/{kind}/{org}/{manifest_id}.json"
    sha = put(key, canonical(body), "application/json")
    with database() as conn:
        changed = conn.execute(
            """INSERT INTO manifests(id,org_id,batch_id,kind,object_key,checksum,body)
                     VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (manifest_id, org, batch, kind, key, sha, Jsonb(body)),
        ).rowcount
        if changed:
            conn.execute(
                "INSERT INTO audit_events(org_id,batch_id,event,details) VALUES(%s,%s,'manifest.published',%s)",
                (org, batch, Jsonb({"kind": kind, "manifest_id": manifest_id})),
            )
    return {"manifest_id": manifest_id, "manifest_key": key, **body}
