FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip show flask redis gunicorn python-json-logger prometheus_client || { echo "Dependencies missing"; exit 1; }

# Copy application code
COPY gateway.py .
RUN mkdir -p /app/logs

# Expose port
EXPOSE 8080

# Run with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--config", "/dev/null", "--log-level", "debug", "gateway:app"]
