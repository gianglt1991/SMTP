import redis
import time
import logging
import os
import signal
from datetime import datetime
from os import getenv
from logging.handlers import RotatingFileHandler
from tenacity import retry, stop_after_attempt, wait_exponential, stop_after_delay
from prometheus_client import Gauge, Counter, start_http_server

# Configure logging
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"mailq_logger_{datetime.now().strftime('%Y%m%d')}.log")
handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
logging.basicConfig(
    level=getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] [Queue Length] %(message)s",
    handlers=[handler, logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Prometheus metrics
TOTAL_CHECKS = Counter("queue_checks_total", "Total number of queue checks")
QUEUE_LENGTH = Gauge("queue_length", "Current length of email_jobs queue")

# Handle graceful shutdown
def handle_shutdown(signum, frame):
    logger.info("Shutting down gracefully")
    exit(0)

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def check_queue_length(r):
    return r.llen("email_jobs")

@retry(stop=stop_after_delay(300))
def main():
    # Start Prometheus HTTP server
    start_http_server(8000)
    redis_url = getenv("REDIS_URL", "redis://queue:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    while True:
        try:
            length = check_queue_length(r)
            logger.info(f"Queue length: {length}")
            QUEUE_LENGTH.set(length)
            TOTAL_CHECKS.inc()
            if length > 1000:
                logger.warning(f"High queue length detected: {length}")
        except redis.RedisError as e:
            logger.error(f"Redis error: {e}")
        time.sleep(10)

if __name__ == "__main__":
    main()
