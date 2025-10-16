# python
# worker/main.py
# Background worker that consumes jobs from Redis, processes them, updates job hash metadata,
# and exposes Prometheus metrics via prometheus_client HTTP server.
import os
import time
import random
import threading
import redis
from prometheus_client import Counter, Histogram, start_http_server

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs_queue")
METRICS_PORT = int(os.environ.get("METRICS_PORT", 8000))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

PROCESSED = Counter("worker_jobs_processed_total", "Total number of jobs processed by worker")
PROCESSING_TIME = Histogram("worker_job_processing_seconds", "Job processing time seconds")

def process_job(job_id: str):
    job_key = f"job:{job_id}"
    # mark started
    r.hset(job_key, mapping={"status": "processing", "started_at": str(time.time())})
    # simulate work (randomized for variable processing time)
    work_time = random.uniform(0.2, 2.0)
    with PROCESSING_TIME.time():
        time.sleep(work_time)
    # update result
    r.hset(job_key, mapping={
        "status": "done",
        "finished_at": str(time.time()),
        "result": f"processed in {work_time:.2f}s"
    })
    PROCESSED.inc()

def worker_loop():
    # blocking pop: BRPOP returns (queue, job_id) or None
    while True:
        try:
            item = r.brpop(QUEUE_KEY, timeout=5)  # wait up to 5s for a job
            if item:
                _, job_id = item
                process_job(job_id)
            else:
                # timeout - loop again
                continue
        except Exception as e:
            # log and sleep briefly on error
            print("Worker error:", e)
            time.sleep(1)

if __name__ == "__main__":
    # Start Prometheus metrics server on separate thread (HTTP)
    start_http_server(METRICS_PORT)
    print(f"Worker metrics exposed on :{METRICS_PORT}/")
    # Start worker loop (blocking)
    worker_loop()