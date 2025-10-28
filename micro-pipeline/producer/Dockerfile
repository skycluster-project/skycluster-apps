# Dockerfile for producer
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy app
COPY main.py /app/producer.py

# Install runtime dependencies
RUN pip install --no-cache-dir prometheus_client requests redis

EXPOSE 9000

CMD ["python", "producer.py"]