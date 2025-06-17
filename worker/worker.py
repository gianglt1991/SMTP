import redis
import json
import time
import smtplib
import logging
import os
import random
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pythonjsonlogger import jsonlogger
from os import getenv
import dkim

# Configure logging
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"worker_{time.strftime('%Y%m%d')}.log")
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

def load_smtp_configs(config_file, secrets_path="/run/secrets"):
    """Load SMTP configurations and credentials from secrets."""
    try:
        with open(config_file) as f:
            configs = json.load(f)
        for config in configs:
            secret_file = os.path.join(secrets_path, f"smtp_{config['id']}_user")
            if os.path.exists(secret_file):
                with open(secret_file) as f:
                    config["user"] = f.read().strip()
            secret_file = os.path.join(secrets_path, f"smtp_{config['id']}_pass")
            if os.path.exists(secret_file):
                with open(secret_file) as f:
                    config["pass"] = f.read().strip()
        return configs
    except Exception as e:
        logger.critical(f"Failed to load SMTP configs: {e}")
        raise

def select_smtp_config(configs, r):
    """Select SMTP config based on weights and blacklist status."""
    valid_configs = []
    total_weight = 0
    for config in configs:
        if not r.sismember("blacklisted_ips", config["host"]):
            weight = config.get("weight", 1.0)
            valid_configs.append((config, weight))
            total_weight += weight
    if not valid_configs:
        raise ValueError("No valid SMTP configurations available")
    weights = [w / total_weight for _, w in valid_configs]
    return random.choices([c for c, _ in valid_configs], weights=weights)[0]

def sign_dkim(msg, domain, selector, private_key_path):
    """Sign email with DKIM."""
    try:
        headers = [b"To", b"From", b"Subject"]
        with open(private_key_path, "rb") as f:
            private_key = f.read()
        sig = dkim.sign(
            message=msg.as_string().encode(),
            selector=selector.encode(),
            domain=domain.encode(),
            privkey=private_key,
            include_headers=headers
        )
        msg["DKIM-Signature"] = sig.decode().replace("\r\n", " ").strip()
        logger.debug(f"DKIM signed for domain {domain}")
    except Exception as e:
        logger.error(f"DKIM signing failed: {e}")

def main():
    try:
        # Configuration from environment variables
        redis_url = getenv("QUEUE_URL", "redis://queue:6379/0")
        job_queue = getenv("JOB_QUEUE", "email_jobs")
        delivered_queue = getenv("DELIVERED_QUEUE", "delivered")
        failed_queue = getenv("FAILED_QUEUE", "failed_jobs")
        bounced_queue = getenv("BOUNCED_QUEUE", "bounced")
        smtp_config_file = getenv("SMTP_CONFIG_FILE", "/app/smtp_rotation.json")
        blpop_timeout = int(getenv("BLPOP_TIMEOUT", "5"))
        dkim_key_path = getenv("DKIM_KEY_PATH", "/app/keys/yourdomain.com.mail.private")
        dkim_domain = getenv("DKIM_DOMAIN", "yourdomain.com")
        dkim_selector = getenv("DKIM_SELECTOR", "mail")

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        smtp_configs = load_smtp_configs(smtp_config_file)
        logger.info("Worker started")

        while True:
            try:
                job = r.blpop(job_queue, timeout=blpop_timeout)
                if not job:
                    continue

                data = json.loads(job[1])
                job_id = data.get("job_id", "unknown")
                logger.info(f"Processing job {job_id}")

                # Validate job data
                required_fields = ["from", "to", "subject", "body"]
                if not all(field in data for field in required_fields):
                    logger.error(f"Invalid job data: {data}")
                    r.rpush(failed_queue, json.dumps(data))
                    continue

                # Check rate limit
                sender = data["from"]
                rate_key = f"rate_limit:{sender}"
                count = r.incr(rate_key)
                if count == 1:
                    r.expire(rate_key, 3600)  # 1-hour window
                if count > 100:  # 100 emails/hour
                    logger.warning(f"Rate limit exceeded for {sender}")
                    r.rpush(failed_queue, json.dumps(data))
                    continue

                # Select SMTP server
                smtp_conf = select_smtp_config(smtp_configs, r)
                logger.debug(f"Selected SMTP: {smtp_conf['host']}:{smtp_conf['port']}")

                # Build email
                msg = MIMEMultipart()
                msg["From"] = data["from"]
                msg["To"] = ", ".join(data["to"] if isinstance(data["to"], list) else [data["to"]])
                msg["Subject"] = data["subject"]
                msg.attach(MIMEText(data["body"], "plain"))

                # Sign DKIM
                sign_dkim(msg, dkim_domain, dkim_selector, dkim_key_path)

                # Send email
                with smtplib.SMTP(smtp_conf["host"], smtp_conf["port"]) as smtp:
                    smtp.ehlo()
                    if smtp_conf.get("user") and smtp_conf.get("pass"):
                        smtp.starttls()
                        smtp.login(smtp_conf["user"], smtp_conf["pass"])
                    smtp.sendmail(data["from"], data["to"], msg.as_string())
                    logger.info(f"Job {job_id} delivered")
                    r.rpush(delivered_queue, json.dumps(data))
                    r.incr("worker_metrics:deliveries")

            except smtplib.SMTPResponseException as e:
                error_data = {"error": str(e), "smtp_code": e.smtp_code, **data}
                data["retries"] = data.get("retries", 0) + 1
                logger.error(f"SMTP error for job {job_id}: {e}")
                r.rpush(failed_queue, json.dumps(data))
                r.rpush(bounced_queue, json.dumps(error_data))
                r.incr("worker_metrics:smtp_errors")
                if e.smtp_code >= 500:  # Permanent failure
                    r.incr("worker_metrics:permanent_failures")
            except redis.RedisError as e:
                logger.error(f"Redis error: {e}")
                r.incr("worker_metrics:redis_errors")
                time.sleep(5)
            except Exception as e:
                data["retries"] = data.get("retries", 0) + 1
                logger.error(f"Unexpected error for job {job_id}: {e}")
                r.rpush(failed_queue, json.dumps(data))
                r.rpush(bounced_queue, json.dumps({"error": str(e), **data}))
                r.incr("worker_metrics:unexpected_errors")

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()