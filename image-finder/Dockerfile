# syntax=docker/dockerfile:1.6
FROM debian:12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PATH="/usr/local/google-cloud-sdk/bin:${PATH}"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Base runtime deps (keep minimal). Also install gnupg temporarily for MS repo key.
# set -eux; \
RUN apt-get update; \
    apt-get install -y --no-install-recommends bash ca-certificates curl jq unzip python3 gnupg tzdata; \
    update-ca-certificates; \
    ln -s /usr/bin/python3 /usr/bin/python; \
    mkdir -p /etc/apt/keyrings; \
    curl -4fsSL --retry 20 --retry-all-errors --retry-delay 2 --connect-timeout 7 https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor >/etc/apt/keyrings/microsoft.gpg; \
    chmod go+r /etc/apt/keyrings/microsoft.gpg; \
    rm -rf /var/lib/apt/lists/*;

RUN . /etc/os-release; \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ ${VERSION_CODENAME} main" \
      >/etc/apt/sources.list.d/azure-cli.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends azure-cli; \
    apt-get purge -y gnupg; apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2 (arch-aware), then clean installer artifacts
ARG TARGETARCH
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      "amd64")  AWS_ARCH="x86_64" ;; \
      "arm64")  AWS_ARCH="aarch64" ;; \
      *)        echo "Unsupported TARGETARCH: ${TARGETARCH}"; exit 1 ;; \
    esac; \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}.zip" -o /tmp/awscliv2.zip; \
    unzip -q /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli; \
    rm -rf /tmp/aws /tmp/awscliv2.zip

# Install Google Cloud SDK (arch-aware), then clean tarball
RUN set -eux; \
    case "${TARGETARCH:-amd64}" in \
      "amd64")  GCLOUD_ARCH="x86_64" ;; \
      "arm64")  GCLOUD_ARCH="arm" ;; \
      *)        echo "Unsupported TARGETARCH: ${TARGETARCH}"; exit 1 ;; \
    esac; \
    curl -fsSL "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-${GCLOUD_ARCH}.tar.gz" -o /tmp/gcloud.tar.gz; \
    tar -xzf /tmp/gcloud.tar.gz -C /usr/local; \
    rm -f /tmp/gcloud.tar.gz; \
    ln -sf /usr/local/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud

WORKDIR /app

# Copy all app files in one layer and set permissions concisely
COPY entrypoint.sh aws.sh gcp.sh azure.sh label-mapper.py /app/
RUN chmod +x /app/entrypoint.sh /app/aws.sh /app/gcp.sh /app/azure.sh

ENTRYPOINT ["/app/entrypoint.sh"]
