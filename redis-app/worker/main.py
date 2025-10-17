# python
# worker/main.py
# Multiprocess worker with Prometheus metrics.
import os
import time
import random
import json
import logging
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, Tuple

import redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Configuration from environment variables
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs_queue")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
WORKERS_PER_POD = int(os.environ.get("WORKERS_PER_POD", "2"))
WORK_MODE = os.environ.get("WORK_MODE", "cpu")
SLEEP_MIN = float(os.environ.get("SLEEP_MIN", "0.2"))
SLEEP_MAX = float(os.environ.get("SLEEP_MAX", "2.0"))
CPU_INTENSITY = int(os.environ.get("CPU_INTENSITY", "200000"))
COMPONENT = os.environ.get("COMPONENT", "worker")
DST = f"{REDIS_HOST}:{REDIS_PORT}"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("worker")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Prometheus metrics (main process)
BRPOP_CALLS = Counter("worker_brpop_calls_total", "Total BRPOP calls attempted by worker (main process)")
BRPOP_TIMEOUTS = Counter("worker_brpop_timeouts_total", "Number of BRPOP timeouts (no job found)")
BRPOP_LATENCY = Histogram("worker_brpop_latency_seconds", "Latency of BRPOP calls (seconds)")
IN_FLIGHT = Gauge("worker_in_flight_jobs", "Number of jobs currently being processed (submitted to pool)")
PROCESSED = Counter("worker_jobs_processed_total", "Total number of jobs processed by worker")
PROCESSING_TIME = Histogram("worker_job_processing_seconds", "Histogram of job processing durations (worker subprocess)")
END_TO_END_LATENCY = Histogram(
    "worker_job_end_to_end_latency_seconds",
    "Histogram of end-to-end latency from created_at to finished_at",
    buckets=[0.1, 0.5, 1, 2.5, 5, 10, 100, 300, 600, 1000, 1500, 2000, 3000, 5000, 10000, 20000]
)
LAST_PROCESSED_TS = Gauge("worker_last_processed_timestamp", "Unix timestamp of last processed job")

# Redis metrics (shared naming)
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

# Queue wait histogram (time between job created_at and worker start processing)
WORKER_QUEUE_WAIT = Histogram(
    "worker_queue_wait_seconds",
    "Time a job waited in the queue before worker started processing"
)

def cpu_bound_work(payload: Dict[str, Any], intensity: int) -> Dict[str, Any]:
    start = time.time()
    total = 0
    for i in range(intensity):
        total += (i * i) % 1234577
    processing_time = time.time() - start
    return {"result": f"cpu_work_result:{total}", "processing_time": processing_time}

def io_or_sleep_work(payload: Dict[str, Any], min_s: float, max_s: float) -> Dict[str, Any]:
    start = time.time()
    time.sleep(random.uniform(min_s, max_s))
    processing_time = time.time() - start
    return {"result": "sleep_done", "processing_time": processing_time}

def worker_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    if WORK_MODE == "cpu":
        return cpu_bound_work(payload, CPU_INTENSITY)
    else:
        return io_or_sleep_work(payload, SLEEP_MIN, SLEEP_MAX)

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

def submit_job_to_pool(pool: ProcessPoolExecutor, job_id: str, job_meta: Dict[str, str]):
    payload_raw = job_meta.get("payload", "{}")
    try:
        payload = json.loads(payload_raw.replace("'", '"')) if isinstance(payload_raw, str) else payload_raw
    except Exception:
        payload = {"_raw": payload_raw}
    future = pool.submit(worker_task, payload)
    return future, payload, job_meta

def main_loop():
    # Start prometheus metrics HTTP server
    start_http_server(METRICS_PORT)
    log.info("Prometheus metrics exposed on :%d/ (main process)", METRICS_PORT)

    with ProcessPoolExecutor(max_workers=WORKERS_PER_POD) as pool:
        futures_map = {}

        while True:
            try:
                BRPOP_CALLS.inc()
                brpop_start = time.time()
                item = r.brpop(QUEUE_KEY, timeout=5)
                brpop_elapsed = time.time() - brpop_start
                BRPOP_LATENCY.observe(brpop_elapsed)

                if not item:
                    BRPOP_TIMEOUTS.inc()
                    _drain_completed_futures(futures_map)
                    continue

                _, job_id = item
                if not job_id:
                    log.warning("BRPOP returned empty job id, skipping")
                    continue

                job_key = f"job:{job_id}"
                job_meta = time_redis_cmd(COMPONENT, "HGETALL", DST, r.hgetall, job_key)
                if not job_meta:
                    log.warning("Job key not found for job_id %s, skipping", job_id)
                    continue

                created_at = float(job_meta.get("created_at", time.time()))
                started_at = time.time()
                # queue wait time = when worker got the job minus created_at
                queue_wait = started_at - created_at
                WORKER_QUEUE_WAIT.observe(queue_wait)

                # mark started in redis (lightweight)
                time_redis_cmd(COMPONENT, "HSET", DST, r.hset, job_key, mapping={"status": "processing", "started_at": str(started_at)})

                future, payload, meta = submit_job_to_pool(pool, job_id, job_meta)
                IN_FLIGHT.inc()
                futures_map[future] = {
                    "job_id": job_id,
                    "created_at": created_at,
                    "submit_time": time.time(),
                    "job_key": job_key
                }

                _drain_completed_futures(futures_map)

            except Exception as e:
                log.exception("Main loop error: %s", e)
                time.sleep(1)

def _drain_completed_futures(futures_map):
    done_futures = [f for f in list(futures_map.keys()) if f.done()]
    for fut in done_futures:
        meta = futures_map.pop(fut, None)
        if meta is None:
            continue
        job_id = meta["job_id"]
        job_key = meta["job_key"]
        created_at = meta["created_at"]
        try:
            res = fut.result()
            processing_time = float(res.get("processing_time", 0.0))
            finished_at = time.time()
            # Update job metadata in redis and measure hset latency
            time_redis_cmd(COMPONENT, "HSET", DST, r.hset, job_key, mapping={
                "status": "done",
                "finished_at": str(finished_at),
                "result": str(res.get("result", "")),
                "processing_time": str(processing_time)
            })
            # Update Prometheus metrics
            PROCESSED.inc()
            PROCESSING_TIME.observe(processing_time)
            e2e = finished_at - created_at
            END_TO_END_LATENCY.observe(e2e)
            LAST_PROCESSED_TS.set_to_current_time()
            log.info("Job %s processed (processing_time=%.3fs e2e=%.3fs)", job_id, processing_time, e2e)
        except Exception as exc:
            log.exception("Error processing job %s: %s", job_id, exc)
            time_redis_cmd(COMPONENT, "HSET", DST, r.hset, job_key, mapping={"status": "failed", "error": str(exc)})
        finally:
            IN_FLIGHT.dec()

if __name__ == "__main__":
    main_loop()