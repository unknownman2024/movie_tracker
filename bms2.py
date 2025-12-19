import json
import os
import random
import time
import threading
import signal
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import cloudscraper

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 2
API_TIMEOUT = 12
HARD_TIMEOUT = 15
MAX_RECOVERY_ROUNDS = 5

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
LOG_FILE      = f"{LOG_DIR}/bms{SHARD_ID}.log"

# =====================================================
# LOGGING
# =====================================================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# =====================================================
# HARD TIMEOUT (ANTI-FREEZE)
# =====================================================
class TimeoutError(Exception):
    pass

def _timeout_handler(signum, frame):
    raise TimeoutError("Hard timeout hit")

def hard_timeout(seconds):
    def deco(fn):
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(seconds)
            try:
                return fn(*args, **kwargs)
            finally:
                signal.alarm(0)
        return wrapper
    return deco

# =====================================================
# STATE
# =====================================================
thread_local = threading.local()
all_data = {}
retry_venues = set()   # ONLY real failures go here

# =====================================================
# USER AGENTS
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

# =====================================================
# IDENTITY (STICKY SESSION)
# =====================================================
class Identity:
    def __init__(self):
        self.ua = random.choice(USER_AGENTS)
        self.ip = ".".join(str(random.randint(20, 230)) for _ in range(4))
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )

    def headers(self):
        return {
            "User-Agent": self.ua,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en;q=0.9",
            "Origin": "https://in.bookmyshow.com",
            "Referer": "https://in.bookmyshow.com/",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-ch-ua": '"Chromium";v="120", "Not=A?Brand";v="99"',
            "X-Forwarded-For": self.ip,
        }

def get_identity():
    if not hasattr(thread_local, "identity"):
        log("🧠 Creating new browser identity")
        thread_local.identity = Identity()
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity
    log("🔄 Browser identity reset")

# =====================================================
# API FETCH
# =====================================================
@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(venue_code):
    ident = get_identity()
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )

    r = ident.scraper.get(
        url,
        headers=ident.headers(),
        timeout=API_TIMEOUT
    )

    txt = r.text.strip()
    if not txt.startswith("{"):
        raise RuntimeError("Blocked / HTML response")

    return r.json()

# =====================================================
# PARSER — RETURNS (parsed_data, status)
# =====================================================
def parse_payload(data, venue_code):
    # Case 1: Venue exists but no shows for ANY date
    if data.get("AllShowDatesDisabled") is True:
        log(f"⏩ ALL DATES DISABLED {venue_code}")
        return {}, "ALL_DISABLED"

    sd = data.get("ShowDetails", [])
    if not sd:
        return {}, "NO_SHOWS"

    api_date = sd[0].get("Date")
    if api_date and str(api_date) != str(DATE_CODE):
        log(f"⏩ DATE MISMATCH {venue_code} | API:{api_date} EXPECTED:{DATE_CODE}")
        return {}, "DATE_MISMATCH"

    venue_info = sd[0].get("Venues", {})
    venue_name = venue_info.get("VenueName", "")
    venue_add  = venue_info.get("VenueAdd", "")

    out = defaultdict(list)
    shows = 0

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                show_date = sh.get("ShowDateCode") or (sh.get("ShowDateTime", "")[:8])
                if show_date != DATE_CODE:
                    continue

                total = sold = avail = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free  = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold  += seats - free
                    gross += (seats - free) * price

                shows += 1
                out[movie].append({
                    "venue": venue_name,
                    "address": venue_add,
                    "time": sh.get("ShowTime"),
                    "total": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2),
                })

    if shows:
        return out, "OK"

    return {}, "NO_SHOWS"

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log("🚀 SCRIPT STARTED")

    with open(f"venues{SHARD_ID}.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    venue_codes = list(venues.keys())
    log(f"🎯 Venues loaded: {len(venue_codes)}")

    # ---------------- PASS-1 ----------------
    log("▶ PASS-1 : Primary fetch")
    random.shuffle(venue_codes)

    for i, vcode in enumerate(venue_codes, 1):
        log(f"[P1 {i}/{len(venue_codes)}] {vcode}")
        try:
            raw = fetch_api_raw(vcode)
            parsed, status = parse_payload(raw, vcode)

            if status == "OK":
                all_data[vcode] = parsed
                log(f"✅ FETCHED {vcode}")

            elif status in ("DATE_MISMATCH", "ALL_DISABLED", "NO_SHOWS"):
                log(f"✅ FETCHED ({status}) {vcode}")

            else:
                retry_venues.add(vcode)
                log(f"❌ FAILED {vcode}")

        except Exception as e:
            retry_venues.add(vcode)
            log(f"❌ ERROR {vcode} | {type(e).__name__}")
            reset_identity()

        time.sleep(random.uniform(0.35, 0.75))

    # ---------------- PASS-2 ----------------
    log(f"▶ PASS-2 : Soft retry ({len(retry_venues)})")

    for vcode in list(retry_venues):
        try:
            raw = fetch_api_raw(vcode)
            parsed, status = parse_payload(raw, vcode)

            if status == "OK":
                all_data[vcode] = parsed
                retry_venues.remove(vcode)
                log(f"♻️ RECOVERED {vcode}")

            elif status in ("DATE_MISMATCH", "ALL_DISABLED", "NO_SHOWS"):
                retry_venues.remove(vcode)
                log(f"✅ FETCHED ({status}) {vcode}")

        except Exception:
            time.sleep(random.uniform(0.8, 1.2))

    # ---------------- RECOVERY ----------------
    for round_no in range(1, MAX_RECOVERY_ROUNDS + 1):
        if not retry_venues:
            break

        log(f"🔁 RECOVERY ROUND {round_no} ({len(retry_venues)})")
        reset_identity()

        retry = list(retry_venues)
        random.shuffle(retry)
        time.sleep(min(2 + round_no * 0.8, 6))

        for vcode in retry:
            log(f"🔎 RETRY {vcode}")
            try:
                raw = fetch_api_raw(vcode)
                parsed, status = parse_payload(raw, vcode)

                if status == "OK":
                    all_data[vcode] = parsed
                    retry_venues.remove(vcode)
                    log(f"🛠️ RECOVERED {vcode}")

                elif status in ("DATE_MISMATCH", "ALL_DISABLED", "NO_SHOWS"):
                    retry_venues.remove(vcode)
                    log(f"✅ FETCHED ({status}) {vcode}")

            except Exception as e:
                log(f"⏰ FAIL {vcode} | {type(e).__name__}")
                reset_identity()
                time.sleep(random.uniform(1.2, 2.0))

    # ---------------- SAVE ----------------
    log("💾 Writing output")

    summary = {}
    detailed = []

    for vcode, movies in all_data.items():
        for movie, shows in movies.items():
            m = summary.setdefault(movie, {
                "shows": 0, "gross": 0, "sold": 0, "totalSeats": 0, "venues": set()
            })
            m["venues"].add(vcode)
            for s in shows:
                m["shows"] += 1
                m["gross"] += s["gross"]
                m["sold"] += s["sold"]
                m["totalSeats"] += s["total"]

                detailed.append({
                    "movie": movie,
                    "venue": s["venue"],
                    "time": s["time"],
                    "sold": s["sold"],
                    "total": s["total"],
                    "gross": s["gross"],
                    "date": DATE_CODE
                })

    final_summary = {
        k: {
            "shows": v["shows"],
            "gross": round(v["gross"], 2),
            "sold": v["sold"],
            "totalSeats": v["totalSeats"],
            "venues": len(v["venues"])
        } for k, v in summary.items()
    }

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2)

    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2)

    log(f"✅ DONE — fetched {len(venue_codes) - len(retry_venues)}/{len(venue_codes)} venues")
