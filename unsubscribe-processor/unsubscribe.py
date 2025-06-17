import redis
import json
import time
import logging
import os
from pythonjsonlogger import jsonlogger
from os import getenv
from email_validator import validate_email, EmailNotValidError

# Configure logging
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"unsubscribe_{time.strftime('%Y%m%d')}.log")
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

def load_initial_unsub_list(r, unsub_file, unsub_set_key):
    """Load initial unsubscribe list from file to Redis."""
    try:
        with open(unsub_file) as f:
            emails = json.load(f)
        valid_emails = []
        for email in emails:
            try:
                validate_email(email, check_deliverability=False)
                valid_emails.append(email)
            except EmailNotValidError as e:
                logger.error(f"Invalid email in unsub_list.json: {email}, error: {e}")
        if valid_emails:
            r.sadd(unsub_set_key, *valid_emails)
            logger.info(f"Loaded {len(valid_emails)} emails from {unsub_file} to Redis")
    except FileNotFoundError:
        logger.warning(f"Unsubscribe file {unsub_file} not found")
    except Exception as e:
        logger.error(f"Failed to load unsubscribe file: {e}")

def process_complaints(r, complaint_queue, unsub_set_key):
    """Process complaints from bounced queue to auto-unsubscribe."""
    try:
        job = r.brpop(complaint_queue, timeout=1)
        if not job:
            return
        data = json.loads(job[1])
        smtp_code = data.get("smtp_code", 0)
        email = data.get("to")
        if smtp_code >= 500:  # Permanent failure
            try:
                validate_email(email, check_deliverability=False)
                r.sadd(unsub_set_key, email)
                logger.info(f"Auto-unsubscribed {email} due to permanent failure")
                r.incr("unsubscribe_metrics:auto_unsubscribed")
            except EmailNotValidError:
                logger.error(f"Invalid email in complaint: {email}")
    except redis.RedisError as e:
        logger.error(f"Redis error in complaint processing: {e}")
        r.incr("unsubscribe_metrics:redis_errors")
    except Exception as e:
        logger.error(f"Error processing complaint: {e}")
        r.incr("unsubscribe_metrics:unexpected_errors")

def main():
    try:
        # Configuration from environment variables
        redis_url = getenv("QUEUE_URL", "redis://queue:6379/0")
        job_queue = getenv("JOB_QUEUE", "email_jobs")
        filtered_queue = getenv("FILTERED_QUEUE", "filtered_jobs")
        complaint_queue = getenv("BOUNCED_QUEUE", "bounced")
        unsub_file = getenv("UNSUB_FILE", "/app/unsub_list.json")
        unsub_set_key = getenv("UNSUB_SET_KEY", "unsubscribed_emails")
        blpop_timeout = int(getenv("BLPOP_TIMEOUT", "5"))

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        logger.info("Unsubscribe processor started")

        # Load initial unsubscribe list
        load_initial_unsub_list(r, unsub_file, unsub_set_key)

        while True:
            try:
                # Process complaints periodically
                process_complaints(r, complaint_queue, unsub_set_key)

                # Process email jobs
                job = r.blpop(job_queue, timeout=blpop_timeout)
                if not job:
                    continue

                try:
                    data = json.loads(job[1])
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in job: {job[1]}, error: {e}")
                    r.incr("unsubscribe_metrics:json_errors")
                    continue

                job_id = data.get("job_id", "unknown")
                logger.debug(f"Processing job {job_id}")

                # Validate job data
                if "to" not in data:
                    logger.error(f"Missing 'to' field in job: {data}")
                    r.incr("unsubscribe_metrics:invalid_jobs")
                    continue

                # Handle multiple recipients
                to_addresses = data["to"] if isinstance(data["to"], list) else [data["to"]]
                valid_recipients = []
                for email in to_addresses:
                    try:
                        validate_email(email, check_deliverability=False)
                        if not r.sismember(unsub_set_key, email):
                            valid_recipients.append(email)
                        else:
                            logger.info(f"Skipped unsubscribed recipient: {email}")
                            r.incr("unsubscribe_metrics:skipped")
                    except EmailNotValidError:
                        logger.error(f"Invalid email in job: {email}")
                        r.incr("unsubscribe_metrics:invalid_emails")

                if valid_recipients:
                    data["to"] = valid_recipients if len(valid_recipients) > 1 else valid_recipients[0]
                    r.rpush(filtered_queue, json.dumps(data))
                    logger.info(f"Job {job_id} forwarded to {filtered_queue}")
                    r.incr("unsubscribe_metrics:processed")
                else:
                    logger.info(f"Job {job_id} skipped: all recipients unsubscribed")
                    r.incr("unsubscribe_metrics:skipped_jobs")

            except redis.RedisError as e:
                logger.error(f"Redis error: {e}")
                r.incr("unsubscribe_metrics:redis_errors")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                r.incr("unsubscribe_metrics:unexpected_errors")

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()