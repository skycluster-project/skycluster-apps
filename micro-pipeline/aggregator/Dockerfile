# Dockerfile for aggregator
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy app
COPY main.py /app/aggregator.py

# Install Python deps
RUN pip install --no-cache-dir flask prometheus_client redis

EXPOSE 8001

CMD ["python", "aggregator.py"]