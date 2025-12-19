import json
import os
import random
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
import requests

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 1

API_TIMEOUT = 8          # hard timeout
MAX_RECOVERY_ROUNDS = 6  # more than enough
MAX_WORKERS = 3          # CF safe

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
# STATE
# =====================================================
thread_local = threading.local()
all_data = {}
empty_venues = set()

# =====================================================
# HEADERS
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

def headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
    }

# =====================================================
# CLOUDSCRAPER (SAFE)
# =====================================================
def get_scraper():
    if hasattr(thread_local, "scraper"):
        return thread_local.scraper

    log("🧠 Creating cloudscraper session")

    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )

    # 🔥 warm-up to avoid hang
    try:
        s.get("https://in.bookmyshow.com", timeout=5)
    except Exception:
        pass

    thread_local.scraper = s
    return s

def reset_identity():
    if hasattr(thread_local, "scraper"):
        del thread_local.scraper

# =====================================================
# API FETCH (NEVER HANGS)
# =====================================================
def fetch_api_raw(vcode):
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={vcode}&dateCode={DATE_CODE}"
    )

    log(f"🌐 API CALL {vcode}")

    r = get_scraper().get(
        url,
        headers=headers(),
        timeout=API_TIMEOUT
    )

    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")

    if not r.text.strip().startswith("{"):
        raise RuntimeError("Non-JSON")

    return r.json()

# =====================================================
# PARSER
# =====================================================
def parse_payload(data):
    sd = data.get("ShowDetails", [])
    if not sd:
        return {}

    venue = sd[0].get("Venues", {})
    venue_name = venue.get("VenueName", "")
    venue_add  = venue.get("VenueAdd", "")

    out = defaultdict(list)

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                if (sh.get("ShowDateCode") or "") != DATE_CODE:
                    continue

                total = sold = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free  = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    sold  += seats - free
                    gross += (seats - free) * price

                out[movie].append({
                    "venue": venue_name,
                    "address": venue_add,
                    "time": sh.get("ShowTime"),
                    "sold": sold,
                    "total": total,
                    "gross": round(gross, 2),
                })

    return out

# =====================================================
# RECOVERY WORKER (PARALLEL SAFE)
# =====================================================
def recovery_worker(vcode):
    try:
        time.sleep(random.uniform(0.4, 0.9))
        data = parse_payload(fetch_api_raw(vcode))
        if data:
            return vcode, data
    except Exception:
        pass
    return vcode, None

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log("🚀 SCRIPT STARTED")

    with open("venues1.json", "r") as f:
        venues = json.load(f)

    log(f"🎯 Venues loaded: {len(venues)}")

    # ================= PASS-1 =================
    log("▶ PASS-1 : SERIAL FETCH")
    for i, vcode in enumerate(venues.keys(), 1):
        log(f"[P1 {i}/{len(venues)}] {vcode}")
        try:
            data = parse_payload(fetch_api_raw(vcode))
            if data:
                all_data[vcode] = data
            else:
                empty_venues.add(vcode)
        except Exception as e:
            empty_venues.add(vcode)
            log(f"❌ {vcode} | {e}")

    # ================= PASS-2 =================
    log(f"▶ PASS-2 : SERIAL RETRY ({len(empty_venues)})")
    reset_identity()

    for vcode in list(empty_venues):
        try:
            data = parse_payload(fetch_api_raw(vcode))
            if data:
                all_data[vcode] = data
                empty_venues.remove(vcode)
        except Exception:
            pass

    # ================= PASS-3+ =================
    for rnd in range(1, MAX_RECOVERY_ROUNDS + 1):
        if not empty_venues:
            break

        log(f"🔁 PARALLEL ROUND {rnd} | Pending {len(empty_venues)}")
        reset_identity()
        recovered = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
            futures = [exe.submit(recovery_worker, v) for v in list(empty_venues)]

            for fut in as_completed(futures, timeout=40):
                vcode, data = fut.result()
                if data:
                    all_data[vcode] = data
                    empty_venues.remove(vcode)
                    recovered += 1
                    log(f"🛠️ RECOVERED {vcode}")

        log(f"📊 ROUND {rnd} RECOVERED {recovered}")
        time.sleep(4 if recovered == 0 else 2)

    # ================= SAVE =================
    log("💾 SAVING FILES")

    summary = {}
    detailed = []

    for vcode, movies in all_data.items():
        for movie, shows in movies.items():
            m = summary.setdefault(movie, {
                "shows": 0, "gross": 0,
                "sold": 0, "totalSeats": 0,
                "venues": set()
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

    final = {
        k: {
            "shows": v["shows"],
            "gross": round(v["gross"], 2),
            "sold": v["sold"],
            "totalSeats": v["totalSeats"],
            "venues": len(v["venues"])
        } for k, v in summary.items()
    }

    with open(SUMMARY_FILE, "w") as f:
        json.dump(final, f, indent=2)

    with open(DETAILED_FILE, "w") as f:
        json.dump(detailed, f, indent=2)

    log(f"✅ DONE — {len(all_data)}/{len(venues)} venues fetched")
