FROM python:3.10-slim

COPY requirements.txt /requirements.txt

RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl build-essential python3-venv; \
    python3 -m venv /venv; \
    /venv/bin/python -m pip install --upgrade pip setuptools wheel; \
    /venv/bin/pip install -r /requirements.txt; \
    curl -fsSL "https://storage.googleapis.com/kubernetes-release/release/$(curl -fsSL https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl" -o /usr/local/bin/kubectl; \
    chmod +x /usr/local/bin/kubectl; \
    apt-get purge -y build-essential; apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/* /root/.cache/pip

ENV PATH=/venv/bin:$PATH
ENTRYPOINT ["/bin/bash","-c"]