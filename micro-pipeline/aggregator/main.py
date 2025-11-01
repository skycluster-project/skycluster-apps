# aggregator.py
import os
import base64
import binascii
import time
import redis
import logging
import tempfile
import socket
from flask import Flask, request, jsonify, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError

# Logging config
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("aggregator")

# Config from env
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
AGGREGATOR_LISTEN_PORT = int(os.getenv("AGGREGATOR_LISTEN_PORT", 8001))
JOBS_DIR = os.getenv("JOBS_DIR", "jobs")
MAX_RESULT_BYTES = int(os.getenv("AGGREGATOR_MAX_RESULT_BYTES", 50 * 1024 * 1024))  # 50 MB max save

logger.info("Starting aggregator with REDIS_HOST=%s REDIS_PORT=%s JOBS_DIR=%s", REDIS_HOST, REDIS_PORT, JOBS_DIR)

# Redis client
try:
    if REDIS_URL:
        r = redis.Redis.from_url(REDIS_URL)
        logger.debug("Using Redis from URL")
    else:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        logger.debug("Using Redis host/port")
except Exception:
    logger.exception("Failed to create Redis client")
    r = None

app = Flask(__name__)
os.makedirs(JOBS_DIR, exist_ok=True)
logger.info("Jobs directory ensured at %s", JOBS_DIR)

# Prometheus metrics
FINALIZE_TOTAL = Counter("aggregator_finalized_total", "Total jobs finalized")
FINALIZE_DURATION = Histogram("aggregator_finalize_seconds", "Finalize duration seconds")
FINALIZE_ERRORS = Counter("aggregator_finalize_errors_total", "Finalize errors")
DUPLICATES = Counter("aggregator_duplicate_finalizations_total", "Duplicate finalizations")
SAVED_BYTES = Histogram("aggregator_saved_bytes", "Saved result size in bytes")


def get_node_identity():
    """Return a dict with hostname and best-effort local IP address."""
    hostname = socket.gethostname()
    ip = "unknown"
    try:
        # Trick to get the primary outbound IP without needing external network calls
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        # Connect to a public DNS IP; no data is sent.
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
    except Exception:
        try:
            ip = socket.gethostbyname(hostname)
        except Exception:
            ip = "unknown"
    return {"hostname": hostname, "ip": ip}


def error_response(message, status=500, extra=None):
    body = {"error": message}
    if extra:
        body.update(extra)
    return jsonify(body), status


def check_redis_ready():
    """Return (ready: bool, message: str)."""
    if r is None:
        return False, "redis_client_not_initialized"
    try:
        ok = r.ping()
        return bool(ok), "redis_ping_ok" if ok else "redis_ping_failed"
    except Exception as e:
        logger.debug("Redis ping failed: %s", e)
        return False, f"redis_ping_exception_{type(e).__name__}"


def check_jobs_dir_writable():
    """Return (ready: bool, message: str). Attempts to write to the jobs directory."""
    try:
        fd, path = tempfile.mkstemp(dir=JOBS_DIR, prefix="healthcheck_", text=False)
        os.close(fd)
        os.remove(path)
        return True, "jobs_dir_writable"
    except Exception as e:
        logger.debug("Jobs directory writable check failed: %s", e)
        return False, f"jobs_dir_not_writable_{type(e).__name__}"


@app.route("/live", methods=["GET"])
def liveness():
    logger.debug("Liveness probe requested")
    return jsonify({"status": "alive"}), 200


@app.route("/ready", methods=["GET"])
def readiness():
    logger.debug("Readiness probe requested")
    redis_ok, redis_msg = check_redis_ready()
    jobs_ok, jobs_msg = check_jobs_dir_writable()
    details = {"redis": redis_msg, "jobs_dir": jobs_msg}

    if not redis_ok:
        logger.warning("Readiness failed: %s", redis_msg)
        return jsonify({"ready": False, "details": details}), 503
    if not jobs_ok:
        logger.warning("Readiness failed: %s", jobs_msg)
        return jsonify({"ready": False, "details": details}), 503

    logger.debug("Readiness OK: %s", details)
    return jsonify({"ready": True, "details": details}), 200


@app.route("/health", methods=["GET"])
def health():
    logger.debug("Health (/health) probe requested")
    live = True
    redis_ok, redis_msg = check_redis_ready()
    jobs_ok, jobs_msg = check_jobs_dir_writable()
    ready = redis_ok and jobs_ok
    details = {"redis": redis_msg, "jobs_dir": jobs_msg}
    status_code = 200 if ready else 503
    logger.info("Health check result: live=%s ready=%s details=%s", live, ready, details)
    return jsonify({"live": live, "ready": ready, "details": details}), status_code


@app.route("/finalize", methods=["POST"])
def finalize():
    start = time.time()
    client_ip = request.remote_addr
    content_length = request.content_length
    logger.info("Received /finalize request from %s content_length=%s", client_ip, content_length)

    data = request.get_json(silent=True)
    if data is None:
        logger.warning("Could not parse JSON body from client %s", client_ip)
        FINALIZE_ERRORS.inc()
        return error_response("invalid_json", 400)

    job_id = data.get("job_id")
    processed_b64 = data.get("processed_b64")

    logger.info("Received finalize request for job %s (client=%s)", job_id, client_ip)

    if not job_id or not processed_b64:
        logger.warning("Missing job_id or processed_b64 in finalize request (job_id=%s)", job_id)
        FINALIZE_ERRORS.inc()
        return error_response("missing_job_id_or_processed_b64", 400)

    # Redis availability check
    if r is None:
        logger.error("Redis client not initialized, cannot finalize job %s", job_id)
        FINALIZE_ERRORS.inc()
        return error_response("redis_unavailable", 503)

    # Try reading job status
    try:
        status = r.hget(f"job:{job_id}", "status")
        logger.debug("Read status for job %s: %s", job_id, status)
    except RedisConnectionError:
        logger.exception("Redis connection error reading job status for job %s", job_id)
        FINALIZE_ERRORS.inc()
        return error_response("redis_unavailable", 503)
    except RedisError:
        logger.exception("Redis error reading job status for job %s", job_id)
        FINALIZE_ERRORS.inc()
        return error_response("redis_error", 503)
    except Exception:
        logger.exception("Unexpected error reading job status for job %s", job_id)
        FINALIZE_ERRORS.inc()
        return error_response("internal_error", 500)

    if status and isinstance(status, (bytes, bytearray)) and status.decode() == "finished":
        DUPLICATES.inc()
        logger.info("Duplicate finalization attempt for job %s (client=%s)", job_id, client_ip)
        node = get_node_identity()
        return jsonify({"status": "already_finished", "node": node}), 200

    # Attempt to mark as finalizing (best-effort)
    try:
        r.hset(f"job:{job_id}", "status", "finalizing")
        logger.debug("Set job:%s status=finalizing", job_id)
    except Exception:
        logger.exception("Redis error setting status=finalizing for job %s", job_id)
        FINALIZE_ERRORS.inc()
        # continue — non-fatal

    # Decode processed result
    try:
        processed_bytes = base64.b64decode(processed_b64)
    except (binascii.Error, ValueError) as e:
        logger.warning("Invalid base64 for job %s from client %s: %s", job_id, client_ip, e)
        FINALIZE_ERRORS.inc()
        return error_response("invalid_base64", 400)

    size = len(processed_bytes)
    logger.debug("Decoded payload for job %s: %d bytes", job_id, size)

    if size == 0:
        logger.warning("Empty processed payload for job %s", job_id)
        FINALIZE_ERRORS.inc()
        return error_response("empty_processed_payload", 400)

    if size > MAX_RESULT_BYTES:
        logger.warning("Processed payload too large for job %s: %d bytes (max %d)", job_id, size, MAX_RESULT_BYTES)
        FINALIZE_ERRORS.inc()
        return error_response("payload_too_large", 413)

    path = os.path.join(JOBS_DIR, f"{job_id}.png")
    try:
        with open(path, "wb") as f:
            f.write(processed_bytes)
        SAVED_BYTES.observe(size)
        logger.info("Saved job %s result to %s (%d bytes)", job_id, path, size)
    except OSError:
        logger.exception("File write error saving job %s to %s", job_id, path)
        FINALIZE_ERRORS.inc()
        return error_response("file_write_error", 500)

    redis_update_failed = False
    try:
        r.hset(f"job:{job_id}", mapping={"status": "finished", "result_path": path, "finished_at": str(time.time())})
        logger.debug("Updated Redis record for job %s as finished", job_id)
    except RedisConnectionError:
        logger.exception("Redis connection error updating job %s to finished", job_id)
        FINALIZE_ERRORS.inc()
        redis_update_failed = True
    except RedisError:
        logger.exception("Redis error updating job %s to finished", job_id)
        FINALIZE_ERRORS.inc()
        redis_update_failed = True
    except Exception:
        logger.exception("Unexpected error updating Redis for job %s", job_id)
        FINALIZE_ERRORS.inc()
        redis_update_failed = True

    # Try to publish notification (best-effort)
    try:
        r.publish("notifications", f"job:{job_id}:finished")
        logger.debug("Published notification for job %s", job_id)
    except Exception:
        logger.exception("Redis publish failed for job %s (non-fatal)", job_id)

    FINALIZE_TOTAL.inc()
    elapsed = time.time() - start
    FINALIZE_DURATION.observe(elapsed)
    logger.info("Finalized job %s in %.3f seconds (bytes=%d redis_update_failed=%s)", job_id, elapsed, size, redis_update_failed)

    node = get_node_identity()
    resp = {"status": "finished", "path": path, "bytes": size, "node": node}
    if redis_update_failed:
        resp["warnings"] = ["redis_update_failed"]
    return jsonify(resp), 200


@app.route("/metrics")
def metrics():
    logger.debug("Metrics requested")
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=AGGREGATOR_LISTEN_PORT)