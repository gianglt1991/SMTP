FROM python:3.11-slim

# Create non-root user
RUN useradd -m appuser

WORKDIR /app
COPY export.py requirements.txt ./

# Install dependencies and clean up cache
RUN pip install --no-cache-dir -r requirements.txt

# Create reports directory
RUN mkdir -p /app/reports && chown appuser:appuser /app/reports

# Switch to non-root user
USER appuser

CMD ["python", "export.py"]
