FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY summarizer /app/summarizer

# Create non-root user and make it owner of the app directory
# RUN groupadd --system app && useradd --system --gid app --home /nonexistent --shell /usr/sbin/nologin app && \
#     chown -R app:app /app
# USER app

# Expose default port (can be overridden via environment)
EXPOSE 9100

# Default command: run uvicorn server
CMD ["uvicorn", "summarizer.main:app", "--host", "0.0.0.0", "--port", "9100"]