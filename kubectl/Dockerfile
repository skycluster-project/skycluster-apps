FROM bitnami/kubectl:1.30
USER 0
RUN apt-get update && apt-get install -y --no-install-recommends curl tar make openssl jq ca-certificates && rm -rf /var/lib/apt/lists/*
USER 1001