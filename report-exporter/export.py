import redis
import json
import time
import os
import logging
from datetime import datetime
from pythonjsonlogger import jsonlogger
from os import getenv

# Configure logging
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"report_exporter_{datetime.now().strftime('%Y%m%d')}.log")
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

def cleanup_old_reports(report_dir, max_age_days=30):
    """Delete reports older than max_age_days to manage disk usage."""
    try:
        now = time.time()
        for filename in os.listdir(report_dir):
            filepath = os.path.join(report_dir, filename)
            if os.path.isfile(filepath) and (now - os.path.getmtime(filepath)) > max_age_days * 86400:
                os.remove(filepath)
                logger.info(f"Deleted old report: {filename}")
    except Exception as e:
        logger.error(f"Failed to clean up old reports: {e}")

def main():
    try:
        # Configuration from environment variables
        redis_url = getenv("QUEUE_URL", "redis://queue:6379/0")
        report_interval = int(getenv("REPORT_INTERVAL", "86400"))  # Default: 24 hours
        report_dir = getenv("REPORT_DIR", "/app/reports")
        redis_keys = getenv("REPORT_KEYS", "delivered,bounced,complaints").split(",")
        max_report_age = int(getenv("MAX_REPORT_AGE_DAYS", "30"))

        os.makedirs(report_dir, exist_ok=True)
        r = redis.Redis.from_url(redis_url, decode_responses=True)

        while True:
            try:
                # Clean up old reports
                cleanup_old_reports(report_dir, max_report_age)

                # Fetch data from Redis with pipelining
                pipe = r.pipeline()
                for key in redis_keys:
                    pipe.llen(key)
                    pipe.lrange(key, 0, -1)
                results = pipe.execute()

                # Build report
                report = {}
                for i, key in enumerate(redis_keys):
                    count = results[i * 2]
                    items = results[i * 2 + 1]
                    report[key] = {
                        "count": count,
                        "items": items
                    }

                # Write report
                timestamp = int(time.time())
                report_file = os.path.join(report_dir, f"daily_{timestamp}.json")
                with open(report_file, "w") as f:
                    json.dump(report, f, indent=2)
                logger.info(f"Generated report: {report_file}")

                # Export metrics to Redis for Prometheus
                r.incr("report_metrics:total_generated")
                for key, data in report.items():
                    r.gauge(f"report_metrics:{key}_count", data["count"])

            except redis.RedisError as e:
                logger.error(f"Redis error: {e}")
                r.incr("report_metrics:redis_errors")
            except IOError as e:
                logger.error(f"File I/O error: {e}")
                r.incr("report_metrics:io_errors")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                r.incr("report_metrics:unexpected_errors")

            time.sleep(report_interval)

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()
