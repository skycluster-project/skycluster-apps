# processor.py
import os
import base64
import io
import time
import requests
import redis
from flask import Flask, request, jsonify, Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from PIL import Image

# Config from env
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8001/finalize")
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PROCESSOR_LISTEN_PORT = int(os.getenv("PROCESSOR_LISTEN_PORT", 8000))

# Redis client
if REDIS_URL:
    r = redis.Redis.from_url(REDIS_URL)
else:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

app = Flask(__name__)

# Prometheus metrics
PROC_TOTAL = Counter("processor_jobs_total", "Total jobs received")
PROC_DURATION = Histogram("processor_processing_seconds", "Processing duration seconds")
PROC_ERRORS = Counter("processor_errors_total", "Processing errors")
INPUT_SIZE = Histogram("processor_input_image_size_bytes", "Input image size")
OUTPUT_SIZE = Histogram("processor_output_image_size_bytes", "Processed image size")
LOCK_ACQUIRED = Counter("processor_lock_acquired_total", "Locks acquired")
LOCK_FAILED = Counter("processor_lock_failed_total", "Locks failed")

@app.route("/process", methods=["POST"])
def process():
    PROC_TOTAL.inc()
    data = request.json
    job_id = data["job_id"]
    image_b64 = data["image_b64"]

    got = r.set(f"lock:job:{job_id}", "proc", nx=True, ex=30)
    if not got:
        LOCK_FAILED.inc()
        return jsonify({"status": "already_processing"}), 409
    LOCK_ACQUIRED.inc()
    r.hset(f"job:{job_id}", "status", "processing")
    try:
        img_bytes = base64.b64decode(image_b64)
        INPUT_SIZE.observe(len(img_bytes))
        with PROC_DURATION.time():
            img = Image.open(io.BytesIO(img_bytes))
            img = img.convert("L")
            img = img.resize((max(1, img.width // 2), max(1, img.height // 2)))
            out = io.BytesIO()
            img.save(out, format="PNG")
            processed_bytes = out.getvalue()
        OUTPUT_SIZE.observe(len(processed_bytes))
        r.hset(f"job:{job_id}", mapping={"status": "processed", "processed_at": str(time.time())})
        payload = {"job_id": job_id, "processed_b64": base64.b64encode(processed_bytes).decode("ascii")}
        try:
            requests.post(AGGREGATOR_URL, json=payload, timeout=30)
        except Exception:
            PROC_ERRORS.inc()
        return jsonify({"status": "sent_to_aggregator"})
    except Exception:
        PROC_ERRORS.inc()
        raise
    finally:
        r.delete(f"lock:job:{job_id}")

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PROCESSOR_LISTEN_PORT)