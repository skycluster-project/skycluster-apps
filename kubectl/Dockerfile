FROM bitnamilegacy/kubectl:1.30

USER root

ENV WORKDIR=/work \
    ISTIO_VER=1.27.0 \
    HELM_VER=3.18.6

# Fixed versioned URLs
ENV ISTIO_TGZ=https://github.com/istio/istio/releases/download/${ISTIO_VER}/istio-${ISTIO_VER}-linux-amd64.tar.gz \
    ISTIOCTL_TGZ=https://github.com/istio/istio/releases/download/${ISTIO_VER}/istioctl-${ISTIO_VER}-linux-amd64.tar.gz \
    HELM_TGZ=https://get.helm.sh/helm-v${HELM_VER}-linux-amd64.tar.gz \
    GCLOUD_TGZ=https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz

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
    rm -rf istio.tar.gz; \
    \
    # Install Helm
    curl -fsSL "${HELM_TGZ}" -o helm.tgz; \
    tar -xzf helm.tgz; \
    mv linux-amd64/helm /usr/local/bin/helm; \
    chmod +x /usr/local/bin/helm; \
    rm -rf helm.tgz linux-amd64; \
    # Install gcloud and gke-gcloud-auth-plugin
    curl -fsSL "${GCLOUD_TGZ}" -o gcloud.tar.gz; \
    tar -xzf gcloud.tar.gz -C /usr/local; \
    /usr/local/google-cloud-sdk/install.sh --quiet --additional-components gke-gcloud-auth-plugin; \
    rm -f gcloud.tar.gz; \
    ln -s /usr/local/google-cloud-sdk/bin/gcloud /usr/local/bin/gcloud; \
    ln -s /usr/local/google-cloud-sdk/bin/gke-gcloud-auth-plugin /usr/local/bin/gke-gcloud-auth-plugin

WORKDIR ${WORKDIR}
