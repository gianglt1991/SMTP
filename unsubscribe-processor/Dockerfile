FROM python:3.11-slim

# Create non-root user
RUN useradd -m appuser

WORKDIR /app
COPY unsubscribe.py unsub_list.json requirements.txt ./

# Install dependencies and clean up cache
RUN pip install --no-cache-dir -r requirements.txt

# Create logs directory
RUN mkdir -p /app/logs && chown appuser:appuser /app/logs

# Switch to non-root user
USER appuser

CMD ["python", "unsubscribe.py"]