# builder: create venv, install deps, download kubectl
FROM python:3.10-slim AS builder
ARG VENV=/venv
COPY requirements.txt /requirements.txt
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    python3 -m venv $VENV && \
    $VENV/bin/pip install --no-cache-dir -r /requirements.txt && \
    curl -fsSL "https://storage.googleapis.com/kubernetes-release/release/$(curl -fsSL https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" -o /kubectl && \
    chmod +x /kubectl && \
    rm -rf /var/lib/apt/lists/*

FROM python:3.10-slim
COPY --from=builder /venv /venv
COPY --from=builder /kubectl /usr/local/bin/kubectl
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ENV PATH=/venv/bin:$PATH
ENTRYPOINT ["python"]