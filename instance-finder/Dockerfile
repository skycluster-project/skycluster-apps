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
COPY --chown=appuser:appuser aws.py gcp.py az.py entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# # Environment variables (override at runtime)
# ENV REGION=""
# ENV ZONE=""
# ENV FAMILY=""
# ENV AWS_SECRET_ACCESS_KEY=""
# ENV AWS_ACCESS_KEY_ID=""
# ENV SPOT_LOOKBACK_HOURS="24"

# By default, AWS SDK will read credentials/region from env vars or mounted ~/.aws
# Example runtime:
#   docker run --rm \
#     -e REGION=us-west-1 -e ZONE=us-west-1a -e FAMILY=g5 \
#     -v $HOME/.aws:/home/appuser/.aws:ro \
#     image-name
ENTRYPOINT ["/app/entrypoint.sh"]
