# Use a slim Python base image
FROM python:3.11-slim

# System deps (ca-certificates for AWS endpoints, tzdata optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

# Create app directory and non-root user

# Install Python dependencies
# boto3 pulls in botocore automatically
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir boto3
    
# Install Google Cloud dependencies
RUN pip install --no-cache-dir google-cloud-compute \
    google-api-python-client google-auth

# Install Azure dependencies
RUN pip install --no-cache-dir azure-identity azure-mgmt-compute requests

WORKDIR /app
RUN useradd -m appuser
USER appuser

# Copy the script into the container (expects file name: find_instances.py)
# If your file has a different name, adjust the COPY and ENTRYPOINT lines.
# COPY --chown=appuser:appuser aws.py gcp.py az.py entrypoint.sh /app/
# RUN chmod +x /app/entrypoint.sh

WORKDIR /app
COPY --chmod=755 entrypoint.sh aws.py gcp.py az.py label_mapper.py /app/

ENTRYPOINT ["/app/entrypoint.sh"]
