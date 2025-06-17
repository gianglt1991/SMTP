import redis
import json
import time
import logging
import os
from datetime import datetime
from pythonjsonlogger import jsonlogger
from os import getenv

# Configure logging
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"retry_handler_{datetime.now().strftime('%Y%m%d')}.log")
logger = logging.getLogger(__name__)
logger.setLevel(getenv("LOG_LEVEL", "INFO"))
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def validate_job(data):
    """Validate job data structure."""
    required_fields = ["job_id", "recipient", "sender"]
    return all(field in data for field in required_fields)

def calculate_backoff(retry_count, base_delay=2, max_delay=60):
    """Calculate exponential backoff delay."""
    delay = min(base_delay * (2 ** retry_count), max_delay)
    return delay

def main():
    try:
        # Configuration from environment variables
        redis_url = getenv("QUEUE_URL", "redis://queue:6379/0")
        failed_queue = getenv("FAILED_QUEUE", "failed_jobs")
        retry_queue = getenv("RETRY_QUEUE", "email_jobs")
        dead_letter_queue = getenv("DEAD_LETTER_QUEUE", "permanent_failed")
        max_retries = int(getenv("MAX_RETRIES", "3"))
        base_delay = float(getenv("BASE_DELAY_SECONDS", "2"))
        max_delay = float(getenv("MAX_DELAY_SECONDS", "60"))
        timeout = int(getenv("BLPOP_TIMEOUT", "5"))

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        logger.info("Retry handler started")

        while True:
            try:
                job = r.blpop(failed_queue, timeout=timeout)
                if not job:
                    continue

                job_data = job[1]
                try:
                    data = json.loads(job_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in job: {job_data}, error: {e}")
                    r.incr("retry_metrics:json_errors")
                    continue

                if not validate_job(data):
                    logger.error(f"Invalid job data: {data}")
                    r.incr("retry_metrics:invalid_jobs")
                    r.rpush(dead_letter_queue, job_data)
                    continue

                job_id = data.get("job_id", "unknown")
                retries = data.get("retries", 0)
                logger.info(f"Processing job {job_id}, retry {retries + 1}")

                if retries < max_retries:
                    data["retries"] = retries + 1
                    delay = calculate_backoff(retries, base_delay, max_delay)
                    logger.info(f"Scheduling retry for job {job_id} after {delay}s")
                    time.sleep(delay)  # Apply backoff delay
                    r.rpush(retry_queue, json.dumps(data))
                    r.incr("retry_metrics:retries")
                else:
                    logger.warning(f"Job {job_id} reached max retries, moving to {dead_letter_queue}")
                    r.rpush(dead_letter_queue, json.dumps(data))
                    r.incr("retry_metrics:permanent_failures")

            except redis.RedisError as e:
                logger.error(f"Redis error: {e}")
                r.incr("retry_metrics:redis_errors")
                time.sleep(5)  # Avoid tight loop on Redis failure
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                r.incr("retry_metrics:unexpected_errors")

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
