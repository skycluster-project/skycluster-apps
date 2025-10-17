# python
# gateway/main.py
# FastAPI app that enqueues jobs into Redis and exposes Prometheus metrics.
import os
import time
import uuid
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
import redis
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Config via environment variables with defaults
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs_queue")
COMPONENT = os.environ.get("COMPONENT", "gateway")
DST = f"{REDIS_HOST}:{REDIS_PORT}"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="gateway")

# Prometheus metrics
ENQUEUED = Counter("gateway_jobs_enqueued_total", "Total jobs enqueued via gateway")
ENQUEUE_LATENCY = Histogram("gateway_enqueue_latency_seconds", "Time taken to enqueue job")

# Redis client metrics (shared naming so queries can aggregate by component label)
REDIS_CMD_LATENCY = Histogram(
    "redis_cmd_latency_seconds",
    "Latency of Redis commands",
    ["component", "cmd", "dst"]
)
REDIS_CMD_ERRORS = Counter(
    "redis_cmd_errors_total",
    "Redis commands errors",
    ["component", "cmd", "dst"]
)

class JobRequest(BaseModel):
    payload: dict = {}

def time_redis_cmd(component: str, cmd: str, dst: str, func, *args, **kwargs):
    start = time.time()
    try:
        res = func(*args, **kwargs)
        return res
    except Exception:
        REDIS_CMD_ERRORS.labels(component=component, cmd=cmd, dst=dst).inc()
        raise
    finally:
        REDIS_CMD_LATENCY.labels(component=component, cmd=cmd, dst=dst).observe(time.time() - start)

@app.post("/jobs")
def create_job(req: JobRequest):
    start = time.time()
    job_id = str(uuid.uuid4())
    job_key = f"job:{job_id}"
    job_meta = {
        "id": job_id,
        "status": "pending",
        "created_at": str(time.time()),
        "payload": str(req.payload),
    }
    # Record HSET latency and errors
    time_redis_cmd(COMPONENT, "HSET", DST, r.hset, job_key, mapping=job_meta)
    # Record LPUSH latency and errors (queue push)
    time_redis_cmd(COMPONENT, "LPUSH", DST, r.lpush, QUEUE_KEY, job_id)

    ENQUEUED.inc()
    ENQUEUE_LATENCY.observe(time.time() - start)
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job_key = f"job:{job_id}"
    if not time_redis_cmd(COMPONENT, "EXISTS", DST, r.exists, job_key):
        raise HTTPException(status_code=404, detail="job not found")
    return time_redis_cmd(COMPONENT, "HGETALL", DST, r.hgetall, job_key)

@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)