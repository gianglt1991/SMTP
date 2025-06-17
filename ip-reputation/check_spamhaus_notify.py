import dns.resolver
import time
import smtplib
import logging
import os
import ipaddress
from email.mime.text import MIMEText
from pythonjsonlogger import jsonlogger
from os import getenv
import redis

# Configure logging
log_dir = "/app/logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"ip_reputation_{time.strftime('%Y%m%d')}.log")
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

def load_ips(blacklist_file):
    """Load and validate IPs from blacklist.txt."""
    try:
        with open(blacklist_file, "r") as f:
            ips = [line.strip() for line in f if line.strip()]
        valid_ips = []
        for ip in ips:
            try:
                ipaddress.ip_address(ip)
                valid_ips.append(ip)
            except ValueError:
                logger.error(f"Invalid IP address in blacklist.txt: {ip}")
        return valid_ips
    except Exception as e:
        logger.error(f"Failed to load blacklist.txt: {e}")
        return []

def is_blacklisted(ip, blacklists, r, cache_ttl=3600):
    """Check if IP is listed in any blacklist, with caching."""
    cache_key = f"blacklist_cache:{ip}"
    cached_result = r.get(cache_key)
    if cached_result:
        logger.debug(f"Cache hit for {ip}: {cached_result}")
        return json.loads(cached_result)

    results = {}
    for blacklist in blacklists:
        reversed_ip = '.'.join(reversed(ip.split('.')))
        query = f"{reversed_ip}.{blacklist}"
        try:
            result = dns.resolver.resolve(query, "A")
            results[blacklist] = [str(rdata) for rdata in result]
            logger.warning(f"IP {ip} listed in {blacklist}: {results[blacklist]}")
        except dns.resolver.NXDOMAIN:
            results[blacklist] = []
            logger.debug(f"IP {ip} not listed in {blacklist}")
        except Exception as e:
            logger.error(f"Error checking {ip} in {blacklist}: {e}")
            results[blacklist] = []
            r.incr("ip_reputation_metrics:dns_errors")

    # Cache results
    r.setex(cache_key, cache_ttl, json.dumps(results))
    return results

def send_alert(ip, blacklist_results, smtp_server, smtp_port, email_from, email_to):
    """Send alert email for blacklisted IP."""
    msg_body = f"⚠️ IP {ip} is listed in the following blacklists:\n"
    for blacklist, codes in blacklist_results.items():
        if codes:
            msg_body += f"- {blacklist}: {', '.join(codes)}\n"
    msg_body += "Immediate action recommended."
    
    msg = MIMEText(msg_body)
    msg["Subject"] = f"[Alert] IP Blacklisted - {ip}"
    msg["From"] = email_from
    msg["To"] = email_to

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as smtp:
            smtp.sendmail(email_from, [email_to], msg.as_string())
            logger.info(f"Alert sent to {email_to} for {ip}")
            return True
    except Exception as e:
        logger.error(f"Failed to send alert for {ip}: {e}")
        return False

def main():
    try:
        # Configuration from environment variables
        blacklist_file = getenv("BLACKLIST_FILE", "/app/blacklist.txt")
        check_interval = int(getenv("CHECK_INTERVAL", "60"))
        blacklists = getenv("BLACKLISTS", "zen.spamhaus.org,dnsbl.sorbs.net").split(",")
        smtp_server = getenv("SMTP_SERVER", "smtp1")
        smtp_port = int(getenv("SMTP_PORT", "25"))
        email_from = getenv("EMAIL_FROM", "alert@yourdomain.com")
        email_to = getenv("ADMIN_EMAIL", "admin@yourdomain.com")
        redis_url = getenv("QUEUE_URL", "redis://queue:6379/0")
        cache_ttl = int(getenv("CACHE_TTL", "3600"))
        alert_cooldown = int(getenv("ALERT_COOLDOWN", "3600"))  # 1 hour

        r = redis.Redis.from_url(redis_url, decode_responses=True)
        logger.info("IP reputation monitoring started")

        last_alert = {}  # Track last alert time per IP
        while True:
            ips = load_ips(blacklist_file)
            if not ips:
                logger.warning("No valid IPs loaded from blacklist.txt")
                time.sleep(check_interval)
                continue

            for ip in ips:
                try:
                    blacklist_results = is_blacklisted(ip, blacklists, r, cache_ttl)
                    is_listed = any(result for result in blacklist_results.values())
                    
                    # Update Redis set for blacklisted IPs
                    if is_listed:
                        r.sadd("blacklisted_ips", ip)
                        r.incr("ip_reputation_metrics:blacklisted")
                    else:
                        r.srem("blacklisted_ips", ip)

                    # Send alert if listed and not in cooldown
                    if is_listed and (ip not in last_alert or time.time() - last_alert[ip] > alert_cooldown):
                        if send_alert(ip, blacklist_results, smtp_server, smtp_port, email_from, email_to):
                            last_alert[ip] = time.time()
                            r.incr("ip_reputation_metrics:alerts_sent")
                        else:
                            r.incr("ip_reputation_metrics:alerts_failed")

                except Exception as e:
                    logger.error(f"Error processing IP {ip}: {e}")
                    r.incr("ip_reputation_metrics:unexpected_errors")

            time.sleep(check_interval)

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()