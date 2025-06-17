from flask import Flask, request, jsonify
import redis
import json
import time
import logging
from pythonjsonlogger import jsonlogger
from os import getenv
from python_jose import jwt
from functools import wraps
import uuid

# Configure logging
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"gateway_{time.strftime('%Y%m%d')}.log")
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

app = Flask(__name__)
r = redis.Redis.from_url(getenv("QUEUE_URL", "redis://queue:6379/0"), decode_responses=True)

def require_jwt(f):
    """Decorator to require JWT authentication."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            logger.warning("Missing JWT token")
            return jsonify({"error": "Missing token"}), 401
        try:
            jwt_secret = getenv("JWT_SECRET")
            if not jwt_secret:
                raise ValueError("JWT_SECRET not configured")
            jwt.decode(token, jwt_secret, algorithms=["HS256"])
            logger.debug("JWT verified")
        except jwt.JWTError as e:
            logger.error(f"JWT error: {e}")
            return jsonify({"error": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated

def validate_job(data):
    """Validate email job data."""
    required_fields = ["from", "to", "subject", "body"]
    return all(field in data for field in required_fields)

@app.route("/", methods=["GET"])
def index():
    logger.info("Health check accessed")
    return jsonify({"status": "Gateway API is running", "version": "1.0.0"}), 200

@app.route("/health", methods=["GET"])
def health():
    try:
        r.ping()
        logger.debug("Redis health check passed")
        return jsonify({"status": "healthy", "redis": "connected"}), 200
    except redis.RedisError as e:
        logger.error(f"Redis health check failed: {e}")
        return jsonify({"status": "unhealthy", "error": str(e)}), 503

@app.route("/send", methods=["POST"])
@require_jwt
def send():
    try:
        data = request.json
        if not data:
            logger.warning("Invalid JSON payload")
            return jsonify({"error": "Invalid JSON"}), 400

        if not validate_job(data):
            logger.warning(f"Invalid job data: {data}")
            return jsonify({"error": "Missing required fields"}), 400

        # Check unsubscribe list
        unsub_list = r.smembers("unsubscribed_emails")
        to_addresses = data["to"] if isinstance(data["to"], list) else [data["to"]]
        if any(email in unsub_list for email in to_addresses):
            logger.warning(f"Unsubscribed recipient in job: {to_addresses}")
            return jsonify({"error": "Recipient unsubscribed"}), 400

        # Check blacklisted IPs
        client_ip = request.remote_addr
        if r.sismember("blacklisted_ips", client_ip):
            logger.warning(f"Blacklisted client IP: {client_ip}")
            return jsonify({"error": "Client IP blacklisted"}), 403

        # Rate limiting
        rate_key = f"rate_limit:{client_ip}"
        count = r.incr(rate_key)
        if count == 1:
            r.expire(rate_key, 3600)  # 1-hour window
        if count > 100:  # 100 requests/hour
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return jsonify({"error": "Rate limit exceeded"}), 429

        # Apply template if specified
        template_id = data.get("template_id")
        if template_id:
            template = r.get(f"template:{template_id}")
            if template:
                data["body"] = template.format(**data.get("template_data", {}))
            else:
                logger.warning(f"Template not found: {template_id}")
                return jsonify({"error": "Template not found"}), 404

        # Generate job ID
        job_id = str(uuid.uuid4())
        data["job_id"] = job_id
        data["submitted_at"] = time.time()
        data["client_ip"] = client_ip

        # Queue job
        r.rpush(getenv("JOB_QUEUE", "email_jobs"), json.dumps(data))
        logger.info(f"Job queued: {job_id}")
        r.incr("gateway_metrics:jobs_queued")

        return jsonify({
            "status": "queued",
            "job_id": job_id,
            "submitted_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        }), 202

    except redis.RedisError as e:
        logger.error(f"Redis error: {e}")
        r.incr("gateway_metrics:redis_errors")
        return jsonify({"error": "Queue unavailable"}), 503
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        r.incr("gateway_metrics:unexpected_errors")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    # For development only
    app.run(host="0.0.0.0", port=8080, debug=False)