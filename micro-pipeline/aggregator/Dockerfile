# Dockerfile.aggregator
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY main.py /app/aggregator.py
RUN pip install --no-cache-dir flask redis prometheus_client
EXPOSE 8001
CMD ["python", "aggregator.py"]