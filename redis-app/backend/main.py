# python
# backend/main.py
# FastAPI app to view queue and job metadata, exposing Prometheus metrics (queue length gauge).
import os
import time
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
import redis
import logging
from prometheus_client import Gauge, Histogram, Counter, generate_latest, CONTENT_TYPE_LATEST

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs_queue")
COMPONENT = os.environ.get("COMPONENT", "backend")
DST = f"{REDIS_HOST}:{REDIS_PORT}"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("backend")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="backend")

QUEUE_LENGTH = Gauge("backend_queue_length", "Current Redis queue length observed by backend")
# Redis metrics
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

@app.get("/queue/length")
def queue_length():
    length = time_redis_cmd(COMPONENT, "LLEN", DST, r.llen, QUEUE_KEY)
    QUEUE_LENGTH.set(length)
    return {"queue_length": length}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job_key = f"job:{job_id}"
    if not time_redis_cmd(COMPONENT, "EXISTS", DST, r.exists, job_key):
        raise HTTPException(status_code=404, detail="job not found")
    return time_redis_cmd(COMPONENT, "HGETALL", DST, r.hgetall, job_key)

@app.get("/metrics")
def metrics():
    # Ensure latest queue length is reflected
    try:
        length = time_redis_cmd(COMPONENT, "LLEN", DST, r.llen, QUEUE_KEY)
        QUEUE_LENGTH.set(length)
    except Exception:
        length = 0
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)