# python
# backend/main.py
# FastAPI app to view queue and job metadata, exposing Prometheus metrics (queue length gauge).
import os
import time
from fastapi import FastAPI, HTTPException
import redis
import logging
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs_queue")
QUEUE_LENGTH = Gauge("backend_queue_length", "Current Redis queue length observed by backend")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("worker")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="backend")

@app.get("/queue/length")
def queue_length():
    length = r.llen(QUEUE_KEY)
    QUEUE_LENGTH.set(length)
    return {"queue_length": length}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job_key = f"job:{job_id}"
    if not r.exists(job_key):
        raise HTTPException(status_code=404, detail="job not found")
    return r.hgetall(job_key)

@app.get("/metrics")
def metrics():
    # Ensure latest queue length is reflected
    try:
        length = r.llen(QUEUE_KEY)
        QUEUE_LENGTH.set(length)
    except Exception:
        length = 0
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)