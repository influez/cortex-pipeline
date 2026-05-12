#!/usr/bin/env python3
"""
backfill_gap.py — Missing alert backfill with time-window chunking
Strategy: Query based on the creation_time filter at small intervals (1 hour)
so that deep offset pagination is NOT required → avoids HTTP 500 errors from Cortex.

How to use:
    python3 backfill_gap.py --start "2026-04-01 00:00:00" --end "2026-04-05 23:59:59"

or edit START_UTC / END_UTC below and run it directly.
"""
import requests
import json
import os
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta

# ─── CONFIG ────────────────────────────────────────────────────────────────────
API_KEY  = "YOUR-API-KEY"
API_ID   = "YOUR-API-ID"
BASE     = "https://api-yourtenant.xdr.id.paloaltonetworks.com"
LOGSTASH = "http://localhost:5044"

STATE_FILE = "/home/ubuntu/cortex-pipeline/state/backfill_gap.json"
LOG_FILE   = "/home/ubuntu/cortex-pipeline/logs/backfill_gap.log"

LIMIT           = 100     # Alert per request (max Cortex = 100)
CHUNK_HOURS     = 1       # Window size per iteration (hours) — reduce to 0.25 if still at 500
MAX_RETRIES     = 10      # Retry per API call
PUSH_RETRIES    = 3
INTER_CHUNK_SLEEP = 2     # Seconds delay between chunks

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

# ─── ARGS ──────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Backfill alerts gap from Cortex XDR to Logstash")
parser.add_argument("--start", help='UTC start time, format: "YYYY-MM-DD HH:MM:SS"')
parser.add_argument("--end",   help='UTC end time, format: "YYYY-MM-DD HH:MM:SS"')
parser.add_argument("--chunk-hours", type=float, default=CHUNK_HOURS,
                    help=f"Window size per chunk in hours (default: {CHUNK_HOURS})")
parser.add_argument("--resume", action="store_true",
                    help="Continue from state file if present")
args = parser.parse_args()

if args.chunk_hours:
    CHUNK_HOURS = args.chunk_hours

# ─── TIME PARSE ───────────────────────────────────────────────────────────────
def parse_dt(s):
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=timezone.utc)

def to_ms(dt):
    return int(dt.timestamp() * 1000)

def ms_to_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

if args.start and args.end:
    RANGE_START = parse_dt(args.start)
    RANGE_END   = parse_dt(args.end)
else:
    # ── EDIT HERE if not using CLI argument ──
    # Example: backfill 1 April to 5 April 2026 (fill in the gaps in the chart)
    RANGE_START = parse_dt("2026-04-01 00:00:00")
    RANGE_END   = parse_dt("2026-04-05 23:59:59")

log.info(f"Backfill range: {RANGE_START} → {RANGE_END}")
log.info(f"Chunk size: {CHUNK_HOURS} jam")

# ─── RESUME STATE ──────────────────────────────────────────────────────────────
resume_from_ms = None
os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)

if args.resume and os.path.exists(STATE_FILE):
    with open(STATE_FILE) as f:
        saved = json.load(f)
    resume_from_ms = saved.get("chunk_start_ms")
    if resume_from_ms:
        log.info(f"Resume from chunk: {ms_to_str(resume_from_ms)}")

# ─── HELPERS ───────────────────────────────────────────────────────────────────
def api_post(payload, retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                BASE + "/public_api/v1/alerts/get_alerts/",
                headers=HEADERS,
                json=payload,
                timeout=(10, 120)
            )
            if resp.status_code == 200:
                return resp.json()
            log.warning(f"HTTP {resp.status_code} attempt {attempt}/{retries}: {resp.text[:300]}")
        except requests.exceptions.Timeout:
            log.warning(f"Timeout attempt {attempt}/{retries}")
        except Exception as e:
            log.warning(f"Error attempt {attempt}/{retries}: {e}")

        if attempt < retries:
            # Exponential backoff, max 5 menit
            wait = min(15 * attempt, 300)
            log.info(f"Retry in {wait}s...")
            time.sleep(wait)

    log.error(f"API failed completely after {retries} attempts for this payload")
    return None

def push_to_logstash(alert):
    for attempt in range(1, PUSH_RETRIES + 1):
        try:
            r = requests.post(
                LOGSTASH,
                data=json.dumps(alert),
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if r.status_code < 300:
                return True
        except Exception as e:
            log.warning(f"Logstash error attempt {attempt}: {e}")
        time.sleep(1)
    log.error(f"FAILED to push alert_id={alert.get('alert_id')}")
    return False

# ─── MAIN LOOP ─────────────────────────────────────────────────────────────────
chunk_delta  = timedelta(hours=CHUNK_HOURS)
chunk_start  = RANGE_START
total_pushed = 0
total_chunks = 0
total_skip   = 0

# Count total chunks for progress
total_chunk_count = int((RANGE_END - RANGE_START) / chunk_delta) + 1

while chunk_start < RANGE_END:
    chunk_end = min(chunk_start + chunk_delta, RANGE_END)
    chunk_start_ms = to_ms(chunk_start)
    chunk_end_ms   = to_ms(chunk_end) - 1   # -1ms to use LTE without overlap between chunks

    # Skip processed chunks (resume mode)
    if resume_from_ms and chunk_start_ms < resume_from_ms:
        chunk_start = chunk_end
        continue

    total_chunks += 1
    log.info(f"\n[Chunk {total_chunks}/{total_chunk_count}] "
             f"{ms_to_str(chunk_start_ms)} → {ms_to_str(chunk_end_ms)}")

    # Save progress to state file
    with open(STATE_FILE, "w") as f:
        json.dump({
            "chunk_start_ms":  chunk_start_ms,
            "chunk_start_str": ms_to_str(chunk_start_ms),
            "total_pushed":    total_pushed
        }, f, indent=2)

    # ── Per-chunk pagination (small max offset due to 1 hour window) ──
    offset       = 0
    chunk_pushed = 0
    seen_ids     = set()

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
                        "value":    chunk_start_ms
                    },
                    {
                        "field":    "creation_time",
                        "operator": "lte",
                        "value":    chunk_end_ms
                    }
                ]
            }
        }

        data = api_post(payload)
        if data is None:
            log.error(f"Skip chunk {ms_to_str(chunk_start_ms)} due to API failure")
            break

        alerts      = data.get("reply", {}).get("alerts", [])
        total_count = data.get("reply", {}).get("total_count", 0)

        if not alerts:
            break

        for alert in alerts:
            aid = str(alert.get("alert_id", ""))
            if aid in seen_ids:
                total_skip += 1
                continue
            seen_ids.add(aid)

            # Tag as backfill for filter/monitoring in Kibana
            alert["_backfill"]     = True
            alert["_ingest_time"]  = datetime.now(timezone.utc).isoformat()

            if push_to_logstash(alert):
                chunk_pushed += 1

        offset += len(alerts)
        if offset >= total_count:
            break

        time.sleep(0.3)

    total_pushed += chunk_pushed
    log.info(f"Chunk selesai: pushed={chunk_pushed} | total={total_pushed}")

    chunk_start = chunk_end
    time.sleep(INTER_CHUNK_SLEEP)

# ─── END ───────────────────────────────────────────────────────────────────
log.info(f"\n{'='*60}")
log.info(f"Backfill COMPLETE")
log.info(f"Total chunks    : {total_chunks}")
log.info(f"Total pushed    : {total_pushed}")
log.info(f"Total skip/dedup: {total_skip}")
log.info(f"{'='*60}")

# Delete state file after complete
if os.path.exists(STATE_FILE):
    os.remove(STATE_FILE)
    log.info("State file deleted.")
