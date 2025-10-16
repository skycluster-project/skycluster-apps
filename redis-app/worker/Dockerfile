# dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
ENV METRICS_PORT=8000
EXPOSE 8000
CMD ["python", "main.py"]