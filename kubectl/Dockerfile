FROM bitnami/kubectl:1.30

USER root

ENV WORKDIR=/work \
    ISTIO_VER=1.27.0

# Fixed versioned URLs
ENV ISTIOCTL_TGZ=https://github.com/istio/istio/releases/download/1.27.0/istioctl-1.27.0-linux-amd64.tar.gz

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        curl \
        tar make jq openssl \
        ca-certificates; \
    rm -rf /var/lib/apt/lists/*;

RUN mkdir -p "${WORKDIR}"; \
    cd "${WORKDIR}"; \
    curl -fsSL "${ISTIO_TGZ}" -o istio.tar.gz; \
    tar -xzf istio.tar.gz; \
    curl -fsSL "${ISTIOCTL_TGZ}" -o istioctl.tgz; \
    tar -xzf istioctl.tgz; \
    chmod +x istioctl; \
    mv istioctl /usr/local/bin/istioctl; \
    rm -rf istioctl.tgz; \
    rm -rf istio.tar.gz


WORKDIR ${WORKDIR}
