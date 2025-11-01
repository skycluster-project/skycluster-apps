# Dockerfile.producer
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY main.py /app/producer.py
RUN pip install --no-cache-dir requests redis prometheus_client
EXPOSE 9000
CMD ["python", "producer.py"]