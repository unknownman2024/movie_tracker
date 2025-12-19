import json
import os
import random
import time
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import cloudscraper

# -------- Selenium (lazy, only PASS-3) --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 1
API_TIMEOUT = 12
SEL_TIMEOUT = 25

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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

def headers():
    ip = ".".join(str(random.randint(10, 240)) for _ in range(4))
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
        "X-Forwarded-For": ip,
    }

# =====================================================
# CLOUDSCRAPER
# =====================================================
def get_scraper():
    if hasattr(thread_local, "scraper"):
        return thread_local.scraper

    log("🧠 Creating cloudscraper session")
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    thread_local.scraper = s
    return s

def reset_identity():
    if hasattr(thread_local, "scraper"):
        del thread_local.scraper
    if hasattr(thread_local, "driver"):
        try:
            thread_local.driver.quit()
        except Exception:
            pass
        del thread_local.driver
    log("🔄 Identity reset")

# =====================================================
# API FETCH
# =====================================================
def fetch_api_raw(venue_code):
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    r = get_scraper().get(url, headers=headers(), timeout=API_TIMEOUT)
    if not r.text.strip().startswith("{"):
        raise RuntimeError("Non-JSON response")
    return r.json()

# =====================================================
# PARSER
# =====================================================
def parse_payload(data, venue_code):
    sd = data.get("ShowDetails", [])
    if not sd:
        return {}

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
                    "session_id": sh.get("SessionId"),
                    "audi": sh.get("Attributes", ""),
                    "total": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2),
                })

    return out if shows else {}

# =====================================================
# SELENIUM (PASS-3 ONLY)
# =====================================================
def get_driver():
    if hasattr(thread_local, "driver"):
        return thread_local.driver

    log("🌐 Starting Selenium browser")
    o = Options()
    o.add_argument("--headless=new")
    o.add_argument("--no-sandbox")
    o.add_argument("--disable-dev-shm-usage")

    d = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=o,
    )
    d.set_page_load_timeout(SEL_TIMEOUT)
    thread_local.driver = d
    return d

def fetch_via_selenium(venue_code):
    api_url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    d = get_driver()
    d.get(api_url)
    body = d.page_source.strip()
    if not body.startswith("{"):
        return {}
    return json.loads(body)

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    log("🚀 SCRIPT STARTED")

    with open("venues1.json", "r") as f:
        venues = json.load(f)

    log(f"🎯 Venues loaded: {len(venues)}")

    # ---------------- PASS 1 ----------------
    log("▶ PASS-1 : API fetch")
    for i, vcode in enumerate(venues.keys(), start=1):
        log(f"[P1 {i}/{len(venues)}] {vcode}")
        try:
            raw = fetch_api_raw(vcode)
            log(f"🌐 API response received for {vcode}")
            data = parse_payload(raw, vcode)
            if data:
                all_data[vcode] = data
                log(f"✅ FETCHED {vcode}")
            else:
                empty_venues.add(vcode)
                log(f"⚠️ EMPTY {vcode}")
        except Exception as e:
            empty_venues.add(vcode)
            log(f"❌ ERROR {vcode} | {e}")

    # ---------------- PASS 2 ----------------
    log("🔄 Reset identity ONCE before PASS-2")
    reset_identity()

    log(f"▶ PASS-2 : API retry ({len(empty_venues)})")
    for i, vcode in enumerate(list(empty_venues), start=1):
        log(f"[P2 {i}/{len(empty_venues)}] {vcode}")
        try:
            raw = fetch_api_raw(vcode)
            log(f"🌐 API response received for {vcode}")
            data = parse_payload(raw, vcode)
            if data:
                all_data[vcode] = data
                empty_venues.remove(vcode)
                log(f"♻️ RECOVERED {vcode}")
        except Exception as e:
            log(f"❌ PASS-2 ERROR {vcode} | {e}")

    # ---------------- PASS 3 ----------------
    log(f"▶ PASS-3 : Selenium verify ({len(empty_venues)})")
    for i, vcode in enumerate(list(empty_venues), start=1):
        log(f"[P3 {i}/{len(empty_venues)}] {vcode}")
        try:
            raw = fetch_via_selenium(vcode)
            data = parse_payload(raw, vcode)
            if data:
                all_data[vcode] = data
                empty_venues.remove(vcode)
                log(f"🧠 SELENIUM RECOVERED {vcode}")
        except Exception as e:
            log(f"❌ SEL ERROR {vcode} | {e}")

    # ---------------- SAVE ----------------
    log("💾 Writing output files")

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

    log("✅ DONE — SCRIPT FINISHED CLEANLY")
