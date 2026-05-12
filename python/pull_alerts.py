#!/usr/bin/env python3
"""
pull_alerts.py — Robust incremental puller for Cortex XDR → Logstash
Cursor: creation_time (millisecond epoch), not alert_id
Run via cron every minute: * * * * * python3 pull_alerts.py
"""
import requests
import json
import os
import time
import logging
from datetime import datetime, timezone

# ─── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY  = "YOUR-API-KEY"
API_ID   = "YOUR-API-ID"
BASE     = "https://api-yourtenant.xdr.id.paloaltonetworks.com"
LOGSTASH = "http://localhost:5044"

STATE_FILE = "/home/ubuntu/cortex-pipeline/state/alerts_ts.json"
LOG_FILE   = "/home/ubuntu/cortex-pipeline/logs/pull.log"
HEARTBEAT  = "/home/ubuntu/cortex-pipeline/state/cron_heartbeat.log"

LIMIT        = 100        # Max per page (Cortex XDR max = 100)
MAX_RETRIES  = 5          # Retry API call
PUSH_RETRIES = 3          # Retry Logstash push
# Overlap back 2 minutes to anticipate alerts appearing late in the API
OVERLAP_MS   = 2 * 60 * 1000

HEADERS = {
    "x-xdr-auth-id": API_ID,
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

# ─── LOGGING ───────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── HEARTBEAT ─────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(HEARTBEAT), exist_ok=True)
with open(HEARTBEAT, "a") as f:
    f.write(f"RUN {datetime.now()}\n")

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def ms_to_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def api_post(url, payload, retries=MAX_RETRIES):
    """POST to Cortex XDR with exponential backoff."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, headers=HEADERS, json=payload, timeout=(10, 60))
            if resp.status_code == 200:
                return resp.json()
            log.warning(f"HTTP {resp.status_code} (attempt {attempt}/{retries}): {resp.text[:200]}")
        except requests.exceptions.Timeout:
            log.warning(f"Timeout (attempt {attempt}/{retries})")
        except Exception as e:
            log.warning(f"Error (attempt {attempt}/{retries}): {e}")
        if attempt < retries:
            wait = min(10 * attempt, 120)
            log.info(f"Retry in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"API failed after {retries} attempts: {url}")

def push_to_logstash(alert, retries=PUSH_RETRIES):
    """Push one alert to Logstash with retry."""
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(
                LOGSTASH,
                data=json.dumps(alert),
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if r.status_code < 300:
                return True
            log.warning(f"Logstash HTTP {r.status_code} (attempt {attempt})")
        except Exception as e:
            log.warning(f"Logstash error (attempt {attempt}): {e}")
        time.sleep(1)
    log.error(f"FAIED to push alert_id={alert.get('alert_id')} after {retries} attempts")
    return False

# ─── LOAD STATE ────────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
if os.path.exists(STATE_FILE):
    with open(STATE_FILE) as f:
        state = json.load(f)
    # Back out OVERLAP_MS to anticipate late-arriving alerts
    since_ms = max(0, int(state.get("last_creation_time_ms", 0)) - OVERLAP_MS)
else:
    # First time: withdraw last 10 minutes
    since_ms = now_ms() - (10 * 60 * 1000)

until_ms = now_ms()
log.info(f"Pulling alerts from {ms_to_str(since_ms)} to {ms_to_str(until_ms)}")

# ─── FETCH & PUSH ──────────────────────────────────────────────────────────────
offset       = 0
total_pushed = 0
new_max_ts   = since_ms
seen_ids     = set()   # Dedup in one run (between pages)

while True:
    payload = {
        "request_data": {
            "search_from": offset,
            "search_to":   offset + LIMIT,
            "sort": {
                "field":   "creation_time",
                "keyword": "asc"
            },
            "filters": [
                {
                    "field":    "creation_time",
                    "operator": "gte",
                    "value":    since_ms
                },
                {
                    "field":    "creation_time",
                    "operator": "lte",
                    "value":    until_ms
                }
            ]
        }
    }

    try:
        data = api_post(BASE + "/public_api/v1/alerts/get_alerts/", payload)
    except RuntimeError as e:
        log.error(str(e))
        break

    alerts = data.get("reply", {}).get("alerts", [])
    if not alerts:
        log.info(f"There are no more alerts at offset {offset}. Done.")
        break

    log.info(f"Offset {offset}: dapat {len(alerts)} alerts")

    for alert in alerts:
        aid  = str(alert.get("alert_id", ""))
        ct   = int(alert.get("creation_time", 0))

        # Skip duplikat dalam satu run
        if aid in seen_ids:
            log.debug(f"Skip duplicate alert_id={aid}")
            continue
        seen_ids.add(aid)

        # Add ingest metadata
        alert["_ingest_time"] = datetime.now(timezone.utc).isoformat()

        if push_to_logstash(alert):
            total_pushed += 1
            if ct > new_max_ts:
                new_max_ts = ct

    offset += len(alerts)

    # If there are no more alerts in this window
    total_count = data.get("reply", {}).get("total_count", 0)
    if offset >= total_count:
        break

    time.sleep(0.5)   # Light rate limiting

# ─── SAVE STATE ────────────────────────────────────────────────────────────────
with open(STATE_FILE, "w") as f:
    json.dump({
        "last_creation_time_ms": new_max_ts,
        "last_run":              datetime.now(timezone.utc).isoformat(),
        "last_pushed":           total_pushed
    }, f, indent=2)

log.info(f"Finished. Pushed: {total_pushed} | New cursor: {ms_to_str(new_max_ts)}")
