# Dockerfile for processor
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system deps required by Pillow
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libjpeg-dev zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy app
COPY main.py /app/processor.py

# Install Python deps
RUN pip install --no-cache-dir flask prometheus_client pillow redis requests

EXPOSE 8000

CMD ["python", "processor.py"]