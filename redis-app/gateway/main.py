# python
# gateway/main.py
# Simple FastAPI app that enqueues jobs into Redis and exposes Prometheus metrics.
import os
import time
import uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Config via environment variables with defaults
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs_queue")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="gateway")

# Prometheus metrics
ENQUEUED = Counter("gateway_jobs_enqueued_total", "Total jobs enqueued via gateway")
ENQUEUE_LATENCY = Histogram("gateway_enqueue_latency_seconds", "Time taken to enqueue job")

class JobRequest(BaseModel):
    payload: dict = {}

@app.post("/jobs")
def create_job(req: JobRequest):
    start = time.time()
    job_id = str(uuid.uuid4())
    # store job metadata as Redis hash
    job_key = f"job:{job_id}"
    job_meta = {
        "id": job_id,
        "status": "pending",
        "created_at": str(time.time()),
        "payload": str(req.payload),
    }
    r.hset(job_key, mapping=job_meta)
    # push job id to queue (list)
    r.lpush(QUEUE_KEY, job_id)
    ENQUEUED.inc()
    ENQUEUE_LATENCY.observe(time.time() - start)
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job_key = f"job:{job_id}"
    if not r.exists(job_key):
        raise HTTPException(status_code=404, detail="job not found")
    return r.hgetall(job_key)

@app.get("/metrics")
def metrics():
    # Expose prometheus metrics
    data = generate_latest()
    return  Response(content=data, media_type=CONTENT_TYPE_LATEST)

# uvicorn will serve this app. We keep it simple and synchronous for clarity.
from fastapi.responses import Response