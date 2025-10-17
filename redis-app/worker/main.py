# python
# worker/main.py
# Multiprocess worker with Prometheus metrics.
# Main process performs BRPOP, fetches job metadata, submits the payload to a process pool,
# and updates Prometheus metrics when tasks complete.
import os
import time
import random
import json
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, Tuple

import redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Configuration from environment variables
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
QUEUE_KEY = os.environ.get("QUEUE_KEY", "jobs_queue")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))
WORKERS_PER_POD = int(os.environ.get("WORKERS_PER_POD", "2"))  # number of subprocesses
WORK_MODE = os.environ.get("WORK_MODE", "cpu")  # "sleep" or "cpu"
SLEEP_MIN = float(os.environ.get("SLEEP_MIN", "0.2"))
SLEEP_MAX = float(os.environ.get("SLEEP_MAX", "2.0"))
CPU_INTENSITY = int(os.environ.get("CPU_INTENSITY", "200000"))  # higher -> more CPU time

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("worker")

# Redis client used in the main process only
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Prometheus metrics (exported by the main process)
BRPOP_CALLS = Counter("worker_brpop_calls_total", "Total BRPOP calls attempted by worker (main process)")
BRPOP_TIMEOUTS = Counter("worker_brpop_timeouts_total", "Number of BRPOP timeouts (no job found)")
IN_FLIGHT = Gauge("worker_in_flight_jobs", "Number of jobs currently being processed (submitted to pool)")
PROCESSED = Counter("worker_jobs_processed_total", "Total number of jobs processed by worker")
PROCESSING_TIME = Histogram("worker_job_processing_seconds", "Histogram of job processing durations (worker subprocess)")
END_TO_END_LATENCY = Histogram(
    "worker_job_end_to_end_latency_seconds", 
    "Histogram of end-to-end latency from created_at to finished_at",
    buckets=[0.1, 0.5, 1, 2.5, 5, 10, 100, 300, 600, 1000, 1500, 2000, 3000, 5000, 10000, 20000])
LAST_PROCESSED_TS = Gauge("worker_last_processed_timestamp", "Unix timestamp of last processed job")

def cpu_bound_work(payload: Dict[str, Any], intensity: int) -> Tuple[str, int]:
    """
    CPU bound work simulation. Returns (job_id_str, processing_millis)
    This function runs inside a subprocess.
    """
    start = time.time()
    # perform a simple, deterministic CPU-heavy task based on intensity
    # (not cryptographically meaningful, just CPU burning)
    total = 0
    for i in range(intensity):
        total += (i * i) % 1234577
    # return result as string to avoid Redis/JSON issues
    processing_time = time.time() - start
    return {"result": f"cpu_work_result:{total}", "processing_time": processing_time}

def io_or_sleep_work(payload: Dict[str, Any], min_s: float, max_s: float) -> Tuple[str, float]:
    """
    Simulate I/O-bound or sleep-based work. Runs in subprocess too.
    """
    start = time.time()
    time.sleep(random.uniform(min_s, max_s))
    processing_time = time.time() - start
    return {"result": f"sleep_done", "processing_time": processing_time}

def worker_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Entrypoint for subprocess work. Choose mode by WORK_MODE.
    Must be picklable argument/return values because it's run in a Process.
    """
    if WORK_MODE == "cpu":
        return cpu_bound_work(payload, CPU_INTENSITY)
    else:
        return io_or_sleep_work(payload, SLEEP_MIN, SLEEP_MAX)

def submit_job_to_pool(pool: ProcessPoolExecutor, job_id: str, job_meta: Dict[str, str]):
    """
    Submit the job payload to the pool and return the future + metadata for tracking.
    """
    # Fetch job payload from job_meta (payload was stored as JSON string by gateway)
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

    # ProcessPoolExecutor: subprocesses do the CPU/I/O "work"
    with ProcessPoolExecutor(max_workers=WORKERS_PER_POD) as pool:
        # mapping of future -> job_id and created_at for metric computation
        futures_map = {}

        while True:
            try:
                BRPOP_CALLS.inc()
                item = r.brpop(QUEUE_KEY, timeout=5)  # wait up to 5s
                if not item:
                    BRPOP_TIMEOUTS.inc()
                    # Optionally, we can handle completed futures here to keep metrics up-to-date
                    _drain_completed_futures(futures_map)
                    continue

                _, job_id = item
                if not job_id:
                    log.warning("BRPOP returned empty job id, skipping")
                    continue

                job_key = f"job:{job_id}"
                job_meta = r.hgetall(job_key)
                if not job_meta:
                    log.warning("Job key not found for job_id %s, skipping", job_id)
                    continue

                created_at = float(job_meta.get("created_at", time.time()))
                # mark started in redis (lightweight)
                r.hset(job_key, mapping={"status": "processing", "started_at": str(time.time())})

                future, payload, meta = submit_job_to_pool(pool, job_id, job_meta)
                IN_FLIGHT.inc()
                futures_map[future] = {
                    "job_id": job_id,
                    "created_at": created_at,
                    "submit_time": time.time(),
                    "job_key": job_key
                }

                # handle any already-completed futures to keep responsiveness
                _drain_completed_futures(futures_map)

            except Exception as e:
                log.exception("Main loop error: %s", e)
                time.sleep(1)

def _drain_completed_futures(futures_map):
    """
    Check futures_map for completed futures and update Redis + metrics.
    This is called periodically in the main loop.
    """
    done_futures = [f for f in list(futures_map.keys()) if f.done()]
    for fut in done_futures:
        meta = futures_map.pop(fut, None)
        if meta is None:
            continue
        job_id = meta["job_id"]
        job_key = meta["job_key"]
        created_at = meta["created_at"]
        submit_time = meta["submit_time"]
        try:
            res = fut.result()  # result should be a dict {"result":..., "processing_time": ...}
            processing_time = float(res.get("processing_time", 0.0))
            finished_at = time.time()
            # Update job metadata in redis
            r.hset(job_key, mapping={
                "status": "done",
                "finished_at": str(finished_at),
                "result": str(res.get("result", "")),
                "processing_time": str(processing_time)
            })
            # Update Prometheus metrics
            PROCESSED.inc()
            PROCESSING_TIME.observe(processing_time)
            # end-to-end latency = finished_at - created_at
            e2e = finished_at - created_at
            END_TO_END_LATENCY.observe(e2e)
            LAST_PROCESSED_TS.set_to_current_time()
            log.info("Job %s processed (processing_time=%.3fs e2e=%.3fs)", job_id, processing_time, e2e)
        except Exception as exc:
            log.exception("Error processing job %s: %s", job_id, exc)
            # mark failed
            r.hset(job_key, mapping={"status": "failed", "error": str(exc)})
        finally:
            IN_FLIGHT.dec()

if __name__ == "__main__":
    main_loop()