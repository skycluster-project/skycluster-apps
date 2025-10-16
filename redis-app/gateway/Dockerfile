# dockerfile
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379
EXPOSE 8000
# Use uvicorn to serve FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]