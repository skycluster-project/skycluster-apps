# producer.py
import os
import uuid
import base64
import requests
import redis
import time
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Config from env
PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://localhost:8000/process")
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PRODUCER_METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", 9000))
# jobs per second (float). e.g. 0.016 ~ 1/min, 1.0 ~ 1/sec
PRODUCER_RATE = float(os.getenv("PRODUCER_RATE", "0.5"))

# Redis client
if REDIS_URL:
    r = redis.Redis.from_url(REDIS_URL)
else:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# Prometheus metrics
PRODUCE_TOTAL = Counter("producer_jobs_total", "Total jobs produced")
FETCH_DURATION = Histogram("producer_fetch_duration_seconds", "Time to fetch image")
IMAGE_SIZE = Histogram("producer_image_size_bytes", "Fetched image size in bytes")
PROCESSOR_RESP = Counter("producer_processor_response_total", "Processor response codes", ["status_code"])
PRODUCER_ERRORS = Counter("producer_errors_total", "Producer errors")
PRODUCER_RATE_GAUGE = Gauge("producer_configured_rate", "Configured producer rate (jobs/sec)")

# Expose metrics endpoint
start_http_server(PRODUCER_METRICS_PORT)
PRODUCER_RATE_GAUGE.set(PRODUCER_RATE)

def produce_once():
    PRODUCE_TOTAL.inc()
    job_id = str(uuid.uuid4())
    try:
        claimed = r.set(f"job:{job_id}:claimed", "1", nx=True, ex=60)
        if not claimed:
            PRODUCER_ERRORS.inc()
            return {"error": "job already claimed"}
        r.hset(f"job:{job_id}", mapping={"status": "queued"})
        with FETCH_DURATION.time():
            resp = requests.get("https://picsum.photos/400", timeout=10)
            resp.raise_for_status()
            img_bytes = resp.content
        IMAGE_SIZE.observe(len(img_bytes))
        img_b64 = base64.b64encode(img_bytes).decode("ascii")

        payload = {"job_id": job_id, "image_b64": img_b64}
        proc_resp = requests.post(PROCESSOR_URL, json=payload, timeout=30)
        PROCESSOR_RESP.labels(status_code=str(proc_resp.status_code)).inc()
        return proc_resp.json()
    except Exception:
        PRODUCER_ERRORS.inc()
        # swallow exception to keep loop running, re-raise if you want to crash
        return {"error": "exception"}

def main_loop():
    if PRODUCER_RATE <= 0:
        # if rate is zero or negative, behave as very low rate (1 per minute)
        interval = 60.0
    else:
        interval = 1.0 / PRODUCER_RATE

    while True:
        start = time.time()
        try:
            produce_once()
        except Exception:
            # already counted in PRODUCER_ERRORS; ensure loop continues
            pass
        elapsed = time.time() - start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            # if processing takes longer than interval, loop immediately (rate can't be met)
            continue

if __name__ == "__main__":
    main_loop()