from flask import Flask, request, jsonify
import redis
import json
import time
import logging
import os
import uuid
from pythonjsonlogger import jsonlogger

# ENV
REDIS_URL = os.getenv("QUEUE_URL", "redis://queue:6379/0")
JOB_QUEUE = os.getenv("JOB_QUEUE", "email_jobs")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
API_PORT = int(os.getenv("API_PORT", 8080))

# Logging setup
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"gateway_{time.strftime('%Y%m%d')}.log")
logger = logging.getLogger("gateway")
logger.setLevel(LOG_LEVEL)
formatter = jsonlogger.JsonFormatter(fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Init Flask app
app = Flask(__name__)

# Redis client
try:
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    logger.info(f"Connected to Redis at {REDIS_URL}")
except redis.RedisError as e:
    logger.critical(f"Failed to connect to Redis: {e}")
    raise

def validate_job(data):
    return all(field in data for field in ["from", "to", "subject", "body"])

@app.route("/", methods=["GET"])
def index():
    return jsonify({"status": "Gateway API is running", "version": "2.0.0"}), 200

@app.route("/health", methods=["GET"])
def health():
    try:
        r.ping()
        return jsonify({"status": "healthy", "redis": "connected"}), 200
    except redis.RedisError as e:
        logger.error(f"Redis health failed: {e}")
        return jsonify({"status": "unhealthy", "error": str(e)}), 503

@app.route("/send", methods=["POST"])
def send():
    trace_id = str(uuid.uuid4())

    try:
        data = request.json
        if not data or not validate_job(data):
            logger.warning(f"{trace_id} - Invalid job payload: {data}")
            return jsonify({"error": "Missing or invalid fields"}), 400

        to_list = data["to"] if isinstance(data["to"], list) else [data["to"]]
        unsub = r.smembers("unsubscribed_emails")
        if any(email in unsub for email in to_list):
            logger.warning(f"{trace_id} - Email blocked: {to_list}")
            return jsonify({"error": "Recipient unsubscribed"}), 400

        ip = request.remote_addr
        if r.sismember("blacklisted_ips", ip):
            logger.warning(f"{trace_id} - Blocked IP: {ip}")
            return jsonify({"error": "IP blacklisted"}), 403

        # Rate limit
        key = f"rate_limit:{ip}"
        count = r.incr(key)
        if count == 1:
            r.expire(key, 3600)
        if count > 100:
            logger.warning(f"{trace_id} - Rate limit exceeded: {ip}")
            return jsonify({"error": "Too many requests"}), 429

        # Apply template
        template_id = data.get("template_id")
        if template_id:
            template = r.get(f"template:{template_id}")
            if not template:
                return jsonify({"error": "Template not found"}), 404
            data["body"] = template.format(**data.get("template_data", {}))

        job_id = str(uuid.uuid4())
        data.update({
            "job_id": job_id,
            "submitted_at": time.time(),
            "client_ip": ip
        })

        r.rpush(JOB_QUEUE, json.dumps(data))
        logger.info(f"{trace_id} - Queued job: {job_id}")

        return jsonify({
            "status": "queued",
            "job_id": job_id,
            "submitted_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        }), 202

    except redis.RedisError as e:
        logger.error(f"{trace_id} - Redis error: {e}")
        return jsonify({"error": "Redis unavailable"}), 503
    except Exception as e:
        logger.exception(f"{trace_id} - Unexpected error: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=API_PORT, debug=False)
