# producer.py
import os
import uuid
import base64
import requests
import redis
import time
import logging
import socket
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from requests.exceptions import RequestException, Timeout, ConnectionError as RequestsConnectionError, HTTPError
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError

# Logging config
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("producer")

# Config from env
PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://localhost:8000/process")
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
PRODUCER_METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", 9000))
PRODUCER_RATE = float(os.getenv("PRODUCER_RATE", "0.5"))
MAX_FETCH_BYTES = int(os.getenv("PRODUCER_MAX_FETCH_BYTES", 10 * 1024 * 1024))  # 10 MB
CHECK_PROCESSOR_HEALTH = os.getenv("CHECK_PROCESSOR_HEALTH", "false").lower() in ("1", "true", "yes")
PROCESSOR_HEALTH_PATH = os.getenv("PROCESSOR_HEALTH_PATH", "/metrics")
HEALTH_HTTP_TIMEOUT = float(os.getenv("HEALTH_HTTP_TIMEOUT", "2.0"))

logger.info("Starting producer with PROCESSOR_URL=%s REDIS_HOST=%s REDIS_PORT=%s PRODUCER_RATE=%s CHECK_PROCESSOR_HEALTH=%s",
            PROCESSOR_URL, REDIS_HOST, REDIS_PORT, PRODUCER_RATE, CHECK_PROCESSOR_HEALTH)

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
logger.info("Prometheus metrics server started on port %s", PRODUCER_METRICS_PORT)


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


def check_redis_ready():
    if r is None:
        return False, "redis_client_not_initialized"
    try:
        ok = r.ping()
        return bool(ok), "redis_ping_ok" if ok else "redis_ping_failed"
    except Exception as e:
        logger.debug("Redis ping failed: %s", e)
        return False, f"redis_ping_exception: {type(e).__name__}"


def check_processor_http():
    try:
        from urllib.parse import urlparse
        parsed = urlparse(PROCESSOR_URL)
        base = f"{parsed.scheme}://{parsed.netloc}"
        check_url = base + PROCESSOR_HEALTH_PATH
        resp = requests.get(check_url, timeout=HEALTH_HTTP_TIMEOUT)
        if resp.status_code < 500:
            return True, f"processor_ok_{resp.status_code}"
        return False, f"processor_unhealthy_status_{resp.status_code}"
    except Exception as e:
        logger.debug("Processor HTTP health check failed: %s", e)
        return False, f"processor_check_exception_{type(e).__name__}"


def produce_once():
    PRODUCE_TOTAL.inc()
    job_id = str(uuid.uuid4())
    logger.info("Producing job %s", job_id)

    # Check Redis availability
    if r is None:
        logger.error("Redis client not initialized, skipping produce for job %s", job_id)
        PRODUCER_ERRORS.inc()
        return {"error": "redis_unavailable"}

    try:
        try:
            claimed = r.set(f"job:{job_id}:claimed", "1", nx=True, ex=60)
            logger.debug("Redis claimed set for job %s -> %s", job_id, claimed)
        except Exception:
            logger.exception("Redis error trying to claim job %s", job_id)
            PRODUCER_ERRORS.inc()
            return {"error": "redis_error"}

        if not claimed:
            PRODUCER_ERRORS.inc()
            logger.warning("Job %s already claimed, skipping", job_id)
            return {"error": "job_already_claimed"}

        try:
            r.hset(f"job:{job_id}", mapping={"status": "queued"})
        except Exception:
            logger.exception("Redis error setting initial job status for job %s", job_id)
            PRODUCER_ERRORS.inc()

        # Fetch image from remote source
        try:
            with FETCH_DURATION.time():
                logger.debug("Fetching image for job %s from picsum", job_id)
                resp = requests.get("https://picsum.photos/400", timeout=10)
                resp.raise_for_status()
                img_bytes = resp.content
            fetch_size = len(img_bytes)
            IMAGE_SIZE.observe(fetch_size)
            logger.info("Fetched image for job %s size=%d bytes", job_id, fetch_size)
            if fetch_size == 0:
                logger.warning("Fetched empty image for job %s", job_id)
                PRODUCER_ERRORS.inc()
                return {"error": "empty_image_fetched"}
            if fetch_size > MAX_FETCH_BYTES:
                logger.warning("Fetched image too large for job %s: %d bytes (max %d)", job_id, fetch_size, MAX_FETCH_BYTES)
                PRODUCER_ERRORS.inc()
                return {"error": "fetched_too_large"}
        except RequestsConnectionError:
            logger.exception("Connection error fetching image for job %s", job_id)
            PRODUCER_ERRORS.inc()
            return {"error": "fetch_connection_error"}
        except Timeout:
            logger.exception("Timeout fetching image for job %s", job_id)
            PRODUCER_ERRORS.inc()
            return {"error": "fetch_timeout"}
        except HTTPError as e:
            logger.exception("HTTP error fetching image for job %s: %s", job_id, e)
            PRODUCER_ERRORS.inc()
            return {"error": "fetch_http_error"}
        except RequestException:
            logger.exception("Request exception fetching image for job %s", job_id)
            PRODUCER_ERRORS.inc()
            return {"error": "fetch_error"}
        except Exception:
            logger.exception("Unexpected error fetching image for job %s", job_id)
            PRODUCER_ERRORS.inc()
            return {"error": "fetch_exception"}

        # Prepare and send to processor
        img_b64 = base64.b64encode(img_bytes).decode("ascii")
        payload = {"job_id": job_id, "image_b64": img_b64}
        try:
            send_start = time.time()
            proc_resp = requests.post(PROCESSOR_URL, json=payload, timeout=30)
            send_elapsed = time.time() - send_start
            logger.info("Sent job %s to processor %s status=%s elapsed=%.3f s", job_id, PROCESSOR_URL, proc_resp.status_code, send_elapsed)
            logger.debug("Processor response body (truncated) for job %s: %s", job_id, (proc_resp.text or "")[:1000])
            PROCESSOR_RESP.labels(status_code=str(proc_resp.status_code)).inc()

            # Try parse response JSON and log processor node
            try:
                proc_json = proc_resp.json()
                proc_node = proc_json.get("node")
                if proc_node:
                    msg = f"Processor node for job {job_id}: {proc_node}"
                    logger.info(msg)
                    # Also print so the container stdout shows it clearly
                    print(msg)
                else:
                    logger.debug("Processor response JSON contains no 'node' field for job %s", job_id)
            except ValueError:
                logger.debug("Processor response not JSON for job %s", job_id)
            except Exception:
                logger.exception("Error parsing processor response for job %s", job_id)

            try:
                return proc_resp.json()
            except ValueError:
                logger.debug("Processor response not JSON for job %s", job_id)
                return {"status_code": proc_resp.status_code, "text": proc_resp.text}

        except RequestsConnectionError:
            logger.exception("Connection error sending job %s to processor %s", job_id, PROCESSOR_URL)
            PRODUCER_ERRORS.inc()
            return {"error": "processor_unreachable"}
        except Timeout:
            logger.exception("Timeout sending job %s to processor %s", job_id, PROCESSOR_URL)
            PRODUCER_ERRORS.inc()
            return {"error": "processor_timeout"}
        except RequestException:
            logger.exception("Request exception sending job %s to processor %s", job_id, PROCESSOR_URL)
            PRODUCER_ERRORS.inc()
            return {"error": "processor_error"}
        except Exception:
            logger.exception("Unexpected error sending job %s to processor %s", job_id, PROCESSOR_URL)
            PRODUCER_ERRORS.inc()
            return {"error": "processor_exception"}

    except Exception:
        PRODUCER_ERRORS.inc()
        logger.exception("Unhandled exception producing job %s", job_id)
        return {"error": "exception"}


def main_loop():
    if PRODUCER_RATE <= 0:
        interval = 60.0
    else:
        interval = 1.0 / PRODUCER_RATE

    logger.info("Entering main loop with interval=%s seconds", interval)

    while True:
        start = time.time()
        try:
            produce_once()
        except Exception:
            logger.exception("Exception in produce_once (should not crash main loop)")
            pass
        elapsed = time.time() - start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            logger.debug("Produce loop is behind schedule by %f seconds", -sleep_time)
            continue


if __name__ == "__main__":
    main_loop()