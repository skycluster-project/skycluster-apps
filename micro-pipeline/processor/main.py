# processor.py
import os
import base64
import binascii
import io
import time
import requests
import redis
import logging
import socket
from flask import Flask, request, jsonify, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from PIL import Image, UnidentifiedImageError
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError
from requests.exceptions import RequestException, Timeout, ConnectionError as RequestsConnectionError, HTTPError

# Logging config
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("processor")

# Config from env
AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://aggregator:8001/finalize")
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PROCESSOR_LISTEN_PORT = int(os.getenv("PROCESSOR_LISTEN_PORT", 8000))
MAX_INPUT_BYTES = int(os.getenv("PROCESSOR_MAX_INPUT_BYTES", 10 * 1024 * 1024))  # 10 MB
CHECK_AGGREGATOR_HEALTH = os.getenv("CHECK_AGGREGATOR_HEALTH", "false").lower() in ("1", "true", "yes")
AGGREGATOR_HEALTH_PATH = os.getenv("AGGREGATOR_HEALTH_PATH", "/metrics")
HEALTH_HTTP_TIMEOUT = float(os.getenv("HEALTH_HTTP_TIMEOUT", "2.0"))

logger.info("Starting processor with AGGREGATOR_URL=%s REDIS_HOST=%s REDIS_PORT=%s CHECK_AGGREGATOR_HEALTH=%s",
            AGGREGATOR_URL, REDIS_HOST, REDIS_PORT, CHECK_AGGREGATOR_HEALTH)

# Redis client
try:
    if REDIS_URL:
        r = redis.Redis.from_url(REDIS_URL)
        logger.debug("Using Redis from URL")
    else:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        logger.debug("Using Redis host/port")
except Exception:
    logger.exception("Failed to initialize Redis client")
    r = None

app = Flask(__name__)

# Prometheus metrics
PROC_TOTAL = Counter("processor_jobs_total", "Total jobs received")
PROC_DURATION = Histogram("processor_processing_seconds", "Processing duration seconds")
PROC_ERRORS = Counter("processor_errors_total", "Processing errors")
INPUT_SIZE = Histogram("processor_input_image_size_bytes", "Input image size")
OUTPUT_SIZE = Histogram("processor_output_image_size_bytes", "Processed image size")
LOCK_ACQUIRED = Counter("processor_lock_acquired_total", "Locks acquired")
LOCK_FAILED = Counter("processor_lock_failed_total", "Locks failed")


def get_node_identity():
    hostname = socket.gethostname()
    ip = "unknown"
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
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
    if r is None:
        return False, "redis_client_not_initialized"
    try:
        ok = r.ping()
        return bool(ok), "redis_ping_ok" if ok else "redis_ping_failed"
    except Exception as e:
        logger.debug("Redis ping failed: %s", e)
        return False, f"redis_ping_exception: {type(e).__name__}"


def check_aggregator_http():
    try:
        from urllib.parse import urlparse
        parsed = urlparse(AGGREGATOR_URL)
        base = f"{parsed.scheme}://{parsed.netloc}"
        check_url = base + AGGREGATOR_HEALTH_PATH
        resp = requests.get(check_url, timeout=HEALTH_HTTP_TIMEOUT)
        if resp.status_code < 500:
            return True, f"aggregator_ok_{resp.status_code}"
        return False, f"aggregator_unhealthy_status_{resp.status_code}"
    except Exception as e:
        logger.debug("Aggregator HTTP health check failed: %s", e)
        return False, f"aggregator_check_exception_{type(e).__name__}"


@app.route("/live", methods=["GET"])
def liveness():
    logger.debug("Liveness probe requested")
    return jsonify({"status": "alive"}), 200


@app.route("/ready", methods=["GET"])
def readiness():
    logger.debug("Readiness probe requested")
    redis_ok, redis_msg = check_redis_ready()
    details = {"redis": redis_msg}
    if not redis_ok:
        logger.warning("Readiness failed: %s", redis_msg)
        return jsonify({"ready": False, "details": details}), 503

    if CHECK_AGGREGATOR_HEALTH:
        agg_ok, agg_msg = check_aggregator_http()
        details["aggregator"] = agg_msg
        if not agg_ok:
            logger.warning("Readiness failed (aggregator): %s", agg_msg)
            return jsonify({"ready": False, "details": details}), 503

    logger.debug("Readiness OK: %s", details)
    return jsonify({"ready": True, "details": details}), 200


@app.route("/health", methods=["GET"])
def health():
    logger.debug("Health (/health) probe requested")
    live = True
    redis_ok, redis_msg = check_redis_ready()
    details = {"redis": redis_msg}
    ready = redis_ok
    if ready and CHECK_AGGREGATOR_HEALTH:
        agg_ok, agg_msg = check_aggregator_http()
        details["aggregator"] = agg_msg
        ready = ready and agg_ok
    status_code = 200 if ready else 503
    logger.info("Health check result: live=%s ready=%s details=%s", live, ready, details)
    return jsonify({"live": live, "ready": ready, "details": details}), status_code


@app.route("/process", methods=["POST"])
def process():
    PROC_TOTAL.inc()
    start = time.time()
    client_ip = request.remote_addr
    content_length = request.content_length
    logger.info("Received /process request from %s content_length=%s", client_ip, content_length)

    data = request.get_json(silent=True)
    if data is None:
        logger.warning("Could not parse JSON body from client %s", client_ip)
        PROC_ERRORS.inc()
        return error_response("invalid_json", 400)

    job_id = data.get("job_id")
    image_b64 = data.get("image_b64")

    if not job_id or not image_b64:
        logger.warning("Missing job_id or image_b64 in request (job_id=%s, client=%s)", job_id, client_ip)
        PROC_ERRORS.inc()
        return error_response("missing_job_id_or_image_b64", 400)

    # Check Redis availability early
    if r is None:
        logger.error("Redis client not initialized, cannot acquire lock for job %s", job_id)
        PROC_ERRORS.inc()
        return error_response("redis_unavailable", 503)

    # Acquire lock
    try:
        got = r.set(f"lock:job:{job_id}", "proc", nx=True, ex=30)
        logger.debug("Redis SET nx result for lock:job:%s -> %s", job_id, got)
    except RedisConnectionError:
        logger.exception("Redis connection error trying to set lock for job %s", job_id)
        PROC_ERRORS.inc()
        return error_response("redis_unavailable", 503)
    except RedisError:
        logger.exception("Redis error trying to set lock for job %s", job_id)
        PROC_ERRORS.inc()
        return error_response("redis_error", 503)
    except Exception:
        logger.exception("Unexpected error trying to set lock for job %s", job_id)
        PROC_ERRORS.inc()
        return error_response("internal_error", 500)

    if not got:
        LOCK_FAILED.inc()
        logger.info("Lock already present for job %s, returning 409", job_id)
        return jsonify({"status": "already_processing"}), 409

    LOCK_ACQUIRED.inc()
    logger.debug("Lock acquired for job %s (client=%s)", job_id, client_ip)

    try:
        # Attempt to set processing state (best-effort)
        try:
            r.hset(f"job:{job_id}", "status", "processing")
            logger.debug("Set job:%s status=processing in Redis", job_id)
        except Exception:
            logger.exception("Redis error setting job status to processing for job %s", job_id)
            PROC_ERRORS.inc()

        # Decode image and observe size
        try:
            img_bytes = base64.b64decode(image_b64)
        except (binascii.Error, ValueError) as e:
            logger.warning("Invalid base64 for job %s from client %s: %s", job_id, client_ip, e)
            PROC_ERRORS.inc()
            return error_response("invalid_image_b64", 400)

        input_size = len(img_bytes)
        INPUT_SIZE.observe(input_size)
        logger.info("Processing job %s: input size=%d bytes", job_id, input_size)

        if input_size == 0:
            logger.warning("Empty image payload for job %s", job_id)
            PROC_ERRORS.inc()
            return error_response("empty_image_payload", 400)

        if input_size > MAX_INPUT_BYTES:
            logger.warning("Input payload too large for job %s: %d bytes (max %d)", job_id, input_size, MAX_INPUT_BYTES)
            PROC_ERRORS.inc()
            return error_response("payload_too_large", 413)

        with PROC_DURATION.time():
            try:
                img = Image.open(io.BytesIO(img_bytes))
                logger.debug("Opened image for job %s size=(%s,%s) mode=%s",
                             job_id, getattr(img, "width", "?"), getattr(img, "height", "?"), getattr(img, "mode", "?"))
            except UnidentifiedImageError:
                logger.exception("PIL cannot identify image for job %s", job_id)
                PROC_ERRORS.inc()
                return error_response("invalid_image_data", 400)
            except OSError:
                logger.exception("OS error opening image for job %s", job_id)
                PROC_ERRORS.inc()
                return error_response("invalid_image_data", 400)

            try:
                img = img.convert("L")
                new_width = max(1, img.width // 2)
                new_height = max(1, img.height // 2)
                img = img.resize((new_width, new_height))
                logger.debug("Converted/resized image for job %s new_size=(%s,%s)", job_id, new_width, new_height)
            except Exception:
                logger.exception("Image processing error for job %s", job_id)
                PROC_ERRORS.inc()
                return error_response("processing_error", 500)

            out = io.BytesIO()
            try:
                img.save(out, format="PNG")
            except Exception:
                logger.exception("Failed to save processed image to PNG for job %s", job_id)
                PROC_ERRORS.inc()
                return error_response("processing_error", 500)
            processed_bytes = out.getvalue()

        output_size = len(processed_bytes)
        OUTPUT_SIZE.observe(output_size)
        elapsed = time.time() - start
        logger.info("Finished processing job %s processed_size=%d bytes elapsed=%.3f s", job_id, output_size, elapsed)

        try:
            r.hset(f"job:{job_id}", mapping={"status": "processed", "processed_at": str(time.time())})
            logger.debug("Updated Redis job:%s status=processed", job_id)
        except Exception:
            logger.exception("Redis error updating job status to processed for job %s", job_id)
            PROC_ERRORS.inc()

        # Send to aggregator
        payload = {"job_id": job_id, "processed_b64": base64.b64encode(processed_bytes).decode("ascii")}
        try:
            send_start = time.time()
            resp = requests.post(AGGREGATOR_URL, json=payload, timeout=30)
            send_elapsed = time.time() - send_start
            logger.info("Sent processed job %s to aggregator %s status=%s elapsed=%.3f s", job_id, AGGREGATOR_URL, resp.status_code, send_elapsed)

            # Try parse JSON from aggregator and log its node identity if present
            agg_node = None
            try:
                agg_json = resp.json()
                agg_node = agg_json.get("node")
                if agg_node:
                    logger.info("Aggregator node for job %s: %s", job_id, agg_node)
                else:
                    logger.debug("Aggregator response JSON for job %s contains no 'node' field", job_id)
            except ValueError:
                logger.debug("Aggregator response for job %s not JSON: %s", job_id, (resp.text or "")[:1000])
            except Exception:
                logger.exception("Error parsing aggregator response for job %s", job_id)

            if resp.status_code >= 500:
                logger.warning("Aggregator returned server error %s for job %s", resp.status_code, job_id)
                PROC_ERRORS.inc()
            elif resp.status_code == 503:
                logger.warning("Aggregator reported service unavailable (503) for job %s", job_id)
                PROC_ERRORS.inc()

        except RequestsConnectionError:
            logger.exception("Connection error sending processed job %s to aggregator %s", job_id, AGGREGATOR_URL)
            PROC_ERRORS.inc()
            return error_response("aggregator_unreachable", 503)
        except Timeout:
            logger.exception("Timeout when sending processed job %s to aggregator %s", job_id, AGGREGATOR_URL)
            PROC_ERRORS.inc()
            return error_response("aggregator_timeout", 504)
        except RequestException:
            logger.exception("Request exception when sending processed job %s to aggregator %s", job_id, AGGREGATOR_URL)
            PROC_ERRORS.inc()
            return error_response("aggregator_error", 502)
        except Exception:
            logger.exception("Unexpected error sending processed job %s to aggregator %s", job_id, AGGREGATOR_URL)
            PROC_ERRORS.inc()
            return error_response("aggregator_error", 502)

        # Build response back to caller (producer)
        node = get_node_identity()
        response_body = {"status": "sent_to_aggregator", "bytes": output_size, "node": node}
        if 'agg_node' in locals() and agg_node:
            response_body["aggregator_node"] = agg_node

        return jsonify(response_body), 200

    except Exception:
        PROC_ERRORS.inc()
        logger.exception("Unhandled exception while processing job %s", job_id)
        return error_response("internal_error", 500)
    finally:
        # Ensure lock release
        try:
            if r is not None:
                r.delete(f"lock:job:{job_id}")
                logger.debug("Lock released for job %s", job_id)
        except Exception:
            logger.exception("Failed to release lock for job %s", job_id)


@app.route("/metrics")
def metrics():
    logger.debug("Metrics requested")
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PROCESSOR_LISTEN_PORT)