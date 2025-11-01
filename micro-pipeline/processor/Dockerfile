# Dockerfile.processor
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY main.py /app/processor.py
RUN pip install --no-cache-dir flask pillow redis prometheus_client requests
EXPOSE 8000
CMD ["python", "processor.py"]