# Getting Started

Build and push images into docker hub. Then deploy the app into your cluster.

Deploy `netshoot` in the same namespace and within the pod generate workload:

```bash
for i in $(seq 1 100); do curl -X POST http://gateway:80/jobs -H "Content-Type: application/json" -d '{"payload": {"n": '"$i"'}}'; done
```

```bash
# bash
# Install hey (https://github.com/rakyll/hey)
# 50 concurrent workers sending request for 30 seconds
hey -z 30s -c 50 -m POST -H "Content-Type: application/json" -d '{"payload": {"hello": "world"}}' http://gateway:80/jobs
```


# Deploy Prometheous


```bash

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

# create monitoring namespace and install
kubectl create namespace monitoring || true
helm install kube-prom-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  -f monitoring-values.yaml
```

```bash
kubectl apply -f service-monitors.yaml
kubectl -n monitoring port-forward svc/kube-prom-stack-prometheus 9090:9090
```
Open http://localhost:9090/targets and look for targets for gateway/backend/worker. If they are UP, scraping is working.

```bash
kubectl -n monitoring port-forward svc/kube-prom-stack-grafana 3000:80

# Find the pass:
kubectl --namespace monitoring get secrets kube-prom-stack-grafana -o jsonpath="{.data.admin-password}" | base64 -d ; echo
```

The helm chart usually auto-configures a Prometheus data source named "Prometheus". If not present, add a Prometheus data source:

New → Data source → Prometheus

Create useful Grafana panels (queries) Create a new dashboard in Grafana and add panels with the following queries (set the data source to Prometheus):
Enqueued jobs rate (per second) Query:
rate(gateway_jobs_enqueued_total[1m])

Gateway enqueue latency (P95, over 5m) Query:
histogram_quantile(0.95, sum(rate(gateway_enqueue_latency_seconds_bucket[5m])) by (le))

Backend queue length (current) Query:
backend_queue_length

Worker processed rate (throughput) Query:
rate(worker_jobs_processed_total[1m])

Worker processing time (P95) Query:
histogram_quantile(0.95, sum(rate(worker_job_processing_seconds_bucket[5m])) by (le))

Combined: queue length vs worker throughput (single panel, two queries) Query A (queue length):
backend_queue_length

Query B (throughput per second):

rate(worker_jobs_processed_total[1m])

Pod CPU usage for each component (requires kube-state metrics + node metrics which are included in kube-prometheus-stack) Example (gateway pods):
sum(rate(container_cpu_usage_seconds_total{namespace="redisapp", pod=~"gateway-.*", image!="", container!="POD"}[5m])) by (pod)



# Horizontal Pod Autoscaler

You need to install metric-server:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```

Fix tls insecure connection issue (in development environment):

```bash
kubectl patch deployment metrics-server -n kube-system \
  --type='json' -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls"}]'
```