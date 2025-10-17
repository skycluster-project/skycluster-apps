# summarizer/main.py
# Summarizer service that periodically polls the backend's /metrics and /queue/length,
# computes simple aggregates (rates, quantiles, backlog) and exposes combined Prometheus metrics
# and a /summary JSON endpoint.
#
# Requirements: fastapi, uvicorn, requests, prometheus_client
# Example run:
#   BACKEND_HOST=backend BACKEND_PORT=8000 INTERVAL=10 uvicorn summarizer.main:app --host 0.0.0.0 --port 9100

import os
import time
import threading
import logging
import requests
from typing import Dict, Any, List, Tuple

from fastapi import FastAPI, Response
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.parser import text_string_to_metric_families

# Configuration via environment variables
BACKEND_HOST = os.environ.get("BACKEND_HOST", "backend")
BACKEND_PORT = int(os.environ.get("BACKEND_PORT", 8000))
BACKEND_BASE = os.environ.get("BACKEND_BASE", f"http://{BACKEND_HOST}:{BACKEND_PORT}")
POLL_INTERVAL = float(os.environ.get("INTERVAL", "10.0"))   # seconds
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL)
log = logging.getLogger("summarizer")

app = FastAPI(title="summarizer")

# Prometheus metrics exported by summarizer
SUMMARIZER_POLL_ERRORS = Counter("summarizer_poll_errors_total", "Total backend poll errors")
SUMMARIZER_LAST_POLL_TS = Gauge("summarizer_last_poll_timestamp", "Last successful poll timestamp (unix)")
SUMMARIZER_QUEUE_LENGTH = Gauge("summarizer_queue_length", "Latest observed queue length from backend")
SUMMARIZER_ENQUEUED_TOTAL = Counter("summarizer_enqueued_total", "Observed gateway enqueued counter (monotonic)")
SUMMARIZER_PROCESSED_TOTAL = Counter("summarizer_processed_total", "Observed worker processed counter (monotonic)")
SUMMARIZER_ENQUEUE_RATE = Gauge("summarizer_enqueue_rate_per_second", "Observed enqueue rate (per second, recent interval)")
SUMMARIZER_PROCESS_RATE = Gauge("summarizer_process_rate_per_second", "Observed process rate (per second, recent interval)")
SUMMARIZER_E2E_P50 = Gauge("summarizer_e2e_p50_seconds", "Observed 50th percentile end-to-end latency (s)")
SUMMARIZER_E2E_P90 = Gauge("summarizer_e2e_p90_seconds", "Observed 90th percentile end-to-end latency (s)")
SUMMARIZER_E2E_P99 = Gauge("summarizer_e2e_p99_seconds", "Observed 99th percentile end-to-end latency (s)")
SUMMARIZER_PROC_P50 = Gauge("summarizer_processing_p50_seconds", "Observed 50th percentile processing time (s)")
SUMMARIZER_PROC_P90 = Gauge("summarizer_processing_p90_seconds", "Observed 90th percentile processing time (s)")
SUMMARIZER_PROC_P99 = Gauge("summarizer_processing_p99_seconds", "Observed 99th percentile processing time (s)")

# Internal state for rate computation
_state = {
    "last_poll_ts": None,
    "last_enqueued": None,   # last observed gateway_jobs_enqueued_total
    "last_processed": None,  # last observed worker_jobs_processed_total
    "last_poll_time": None
}

# utility: fetch backend endpoints
def fetch_backend_metrics() -> str:
    url = f"{BACKEND_BASE}/metrics"
    r = requests.get(url, timeout=5)
    r.raise_for_status()
    return r.text

def fetch_queue_length() -> int:
    url = f"{BACKEND_BASE}/queue/length"
    r = requests.get(url, timeout=5)
    r.raise_for_status()
    data = r.json()
    return int(data.get("queue_length", 0))

# parse Prometheus text format and return a mapping of metric_name -> list of samples
# Each sample is (labels_dict, value)
def parse_metrics_text(text: str) -> Dict[str, List[Tuple[Dict[str, str], float]]]:
    families = text_string_to_metric_families(text)
    metrics: Dict[str, List[Tuple[Dict[str, str], float]]] = {}
    for fam in families:
        for sample in fam.samples:
            # sample is a Sample namedtuple: (name, labels, value)
            name, labels, value = sample.name, sample.labels, sample.value
            metrics.setdefault(name, []).append((labels, value))
    return metrics

# get a counter value (no/any labels) by metric name. If multiple label variants exist, returns sum.
def get_counter(metrics: Dict[str, List[Tuple[Dict[str, str], float]]], metric_name: str) -> float:
    vals = metrics.get(metric_name, [])
    if not vals:
        return 0.0
    return sum(v for _, v in vals)

# get histogram components (buckets, count, sum) for a given base histogram name like "worker_job_end_to_end_latency_seconds"
# expects metrics keys: base + "_bucket", base + "_count", base + "_sum"
def get_histogram(metrics: Dict[str, List[Tuple[Dict[str, str], float]]], base_name: str):
    buckets = []  # list of (le, count)
    for labels, value in metrics.get(f"{base_name}_bucket", []):
        # prom bucket samples have a "le" label
        le = labels.get("le")
        try:
            le_f = float(le) if le is not None else float("inf")
        except Exception:
            le_f = float("inf")
        buckets.append((le_f, float(value)))
    # sort buckets by le
    buckets.sort(key=lambda x: x[0])

    # count
    count_vals = metrics.get(f"{base_name}_count", [])
    total_count = sum(v for _, v in count_vals) if count_vals else 0.0

    # sum
    sum_vals = metrics.get(f"{base_name}_sum", [])
    total_sum = sum(v for _, v in sum_vals) if sum_vals else 0.0

    return buckets, total_count, total_sum

# approximate percentile from histogram buckets using cumulative counts
def histogram_percentile(buckets: List[Tuple[float, float]], count: float, q: float) -> float:
    if count == 0:
        return 0.0
    target = q * count
    cum = 0.0
    for le, c in buckets:
        cum += c
        if cum >= target:
            return le
    # fallback: return +Inf or last bucket le
    return buckets[-1][0] if buckets else 0.0

# single poll and update summary metrics
def poll_and_update():
    global _state
    try:
        metrics_text = fetch_backend_metrics()
        metrics = parse_metrics_text(metrics_text)
        queue_len = fetch_queue_length()
    except Exception as e:
        log.warning("Poll error: %s", e)
        SUMMARIZER_POLL_ERRORS.inc()
        return

    now = time.time()
    # counters we care about
    enqueued = get_counter(metrics, "gateway_jobs_enqueued_total")
    processed = get_counter(metrics, "worker_jobs_processed_total")

    # update monotonic counters in our registry by observed delta
    if _state["last_enqueued"] is None:
        # first observation: set initial counters but don't compute rate
        delta_enq = 0.0
    else:
        delta_enq = max(0.0, enqueued - _state["last_enqueued"])

    if _state["last_processed"] is None:
        delta_proc = 0.0
    else:
        delta_proc = max(0.0, processed - _state["last_processed"])

    # if time delta available, compute rates
    if _state["last_poll_time"] is None:
        interval = None
    else:
        interval = now - _state["last_poll_time"]

    # increment our exported counters by delta (so they remain monotonic)
    if delta_enq > 0:
        SUMMARIZER_ENQUEUED_TOTAL.inc(delta_enq)
    if delta_proc > 0:
        SUMMARIZER_PROCESSED_TOTAL.inc(delta_proc)

    # set rates if interval known
    if interval and interval > 0:
        enq_rate = delta_enq / interval
        proc_rate = delta_proc / interval
    else:
        enq_rate = 0.0
        proc_rate = 0.0

    SUMMARIZER_ENQUEUE_RATE.set(enq_rate)
    SUMMARIZER_PROCESS_RATE.set(proc_rate)

    # update queue length gauge
    SUMMARIZER_QUEUE_LENGTH.set(queue_len)

    # compute percentiles for end-to-end and processing histograms
    e2e_buckets, e2e_count, _e2e_sum = get_histogram(metrics, "worker_job_end_to_end_latency_seconds")
    proc_buckets, proc_count, _proc_sum = get_histogram(metrics, "worker_job_processing_seconds")

    e2e_p50 = histogram_percentile(e2e_buckets, e2e_count, 0.50)
    e2e_p90 = histogram_percentile(e2e_buckets, e2e_count, 0.90)
    e2e_p99 = histogram_percentile(e2e_buckets, e2e_count, 0.99)

    proc_p50 = histogram_percentile(proc_buckets, proc_count, 0.50)
    proc_p90 = histogram_percentile(proc_buckets, proc_count, 0.90)
    proc_p99 = histogram_percentile(proc_buckets, proc_count, 0.99)

    SUMMARIZER_E2E_P50.set(e2e_p50)
    SUMMARIZER_E2E_P90.set(e2e_p90)
    SUMMARIZER_E2E_P99.set(e2e_p99)

    SUMMARIZER_PROC_P50.set(proc_p50)
    SUMMARIZER_PROC_P90.set(proc_p90)
    SUMMARIZER_PROC_P99.set(proc_p99)

    SUMMARIZER_LAST_POLL_TS.set(now)

    # update internal state
    _state["last_enqueued"] = enqueued
    _state["last_processed"] = processed
    _state["last_poll_time"] = now

    log.debug("Polled backend: queue=%d enq_total=%.0f proc_total=%.0f enq_rate=%.3f proc_rate=%.3f e2e_p90=%.3f",
              queue_len, enqueued, processed, enq_rate, proc_rate, e2e_p90)

# background thread loop
def poller_loop():
    log.info("Starting poller loop: backend=%s interval=%.1fs", BACKEND_BASE, POLL_INTERVAL)
    while True:
        try:
            poll_and_update()
        except Exception as e:
            log.exception("Unhandled error in poller loop: %s", e)
            SUMMARIZER_POLL_ERRORS.inc()
        time.sleep(POLL_INTERVAL)

# Start poller in background when app starts
@app.on_event("startup")
def startup_event():
    t = threading.Thread(target=poller_loop, daemon=True)
    t.start()
    log.info("Summarizer started")

# Expose metrics endpoint (Prometheus)
@app.get("/metrics")
def metrics():
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)

# Simple JSON summary endpoint for quick inspection
@app.get("/summary")
def summary() -> Dict[str, Any]:
    return {
        "last_poll_ts": _state["last_poll_time"],
        "queue_length": float(SUMMARIZER_QUEUE_LENGTH._value.get()),
        "enqueued_total": float(SUMMARIZER_ENQUEUED_TOTAL._value.get()),
        "processed_total": float(SUMMARIZER_PROCESSED_TOTAL._value.get()),
        "enqueue_rate_per_s": float(SUMMARIZER_ENQUEUE_RATE._value.get()),
        "process_rate_per_s": float(SUMMARIZER_PROCESS_RATE._value.get()),
        "e2e_p50_s": float(SUMMARIZER_E2E_P50._value.get()),
        "e2e_p90_s": float(SUMMARIZER_E2E_P90._value.get()),
        "e2e_p99_s": float(SUMMARIZER_E2E_P99._value.get()),
        "proc_p50_s": float(SUMMARIZER_PROC_P50._value.get()),
        "proc_p90_s": float(SUMMARIZER_PROC_P90._value.get()),
        "proc_p99_s": float(SUMMARIZER_PROC_P99._value.get()),
    }