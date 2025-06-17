FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY gateway.py .
RUN mkdir -p /app/logs

# Expose Prometheus metrics + API port
EXPOSE 8080
EXPOSE 8000

# Default command
CMD ["python", "gateway.py"]

