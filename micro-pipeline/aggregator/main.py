# aggregator.py
import os
import base64
import time
import redis
import os as _os
from flask import Flask, request, jsonify, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Config from env
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
AGGREGATOR_LISTEN_PORT = int(os.getenv("AGGREGATOR_LISTEN_PORT", 8001))

# Redis client
if REDIS_URL:
    r = redis.Redis.from_url(REDIS_URL)
else:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

app = Flask(__name__)
_os.makedirs("jobs", exist_ok=True)

# Prometheus metrics
FINALIZE_TOTAL = Counter("aggregator_finalized_total", "Total jobs finalized")
FINALIZE_DURATION = Histogram("aggregator_finalize_seconds", "Finalize duration seconds")
FINALIZE_ERRORS = Counter("aggregator_finalize_errors_total", "Finalize errors")
DUPLICATES = Counter("aggregator_duplicate_finalizations_total", "Duplicate finalizations")
SAVED_BYTES = Histogram("aggregator_saved_bytes", "Saved result size in bytes")

@app.route("/finalize", methods=["POST"])
def finalize():
    start = time.time()
    data = request.json
    job_id = data["job_id"]
    processed_b64 = data["processed_b64"]
    status = r.hget(f"job:{job_id}", "status")
    if status and status.decode() == "finished":
        DUPLICATES.inc()
        return jsonify({"status": "already_finished"})
    r.hset(f"job:{job_id}", "status", "finalizing")
    try:
        processed_bytes = base64.b64decode(processed_b64)
        path = f"jobs/{job_id}.png"
        with open(path, "wb") as f:
            f.write(processed_bytes)
        SAVED_BYTES.observe(len(processed_bytes))
        r.hset(f"job:{job_id}", mapping={"status": "finished", "result_path": path, "finished_at": str(time.time())})
        r.publish("notifications", f"job:{job_id}:finished")
        FINALIZE_TOTAL.inc()
        FINALIZE_DURATION.observe(time.time() - start)
        return jsonify({"status": "finished", "path": path})
    except Exception:
        FINALIZE_ERRORS.inc()
        raise

@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=AGGREGATOR_LISTEN_PORT)