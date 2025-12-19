import json
import os
import sys
import threading
import random
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import cloudscraper

# -------- Selenium fallback (kept, but not aggressive) --------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# =====================================================
# CONFIG
# =====================================================
NUM_WORKERS = 1              # slow = safer
MAX_ERRORS = 30
SHARD_ID = 1
DUMP_EVERY = 25

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
os.makedirs(BASE_DIR, exist_ok=True)

SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"

lock = threading.Lock()
thread_local = threading.local()

all_data = {}
empty_venues = set()

fetch_count = 0
error_count = 0

# =====================================================
# HEADERS
# =====================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118 Safari/537.36",
]

def random_ip():
    return ".".join(str(random.randint(10, 240)) for _ in range(4))

def headers():
    ip = random_ip()
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
        "X-Forwarded-For": ip,
        "Client-IP": ip,
    }

# =====================================================
# SCRAPER
# =====================================================
def get_scraper():
    if hasattr(thread_local, "scraper"):
        return thread_local.scraper
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

def fetch_cloud(url):
    r = get_scraper().get(url, headers=headers(), timeout=15)
    if not r.text.strip().startswith("{"):
        raise ValueError("HTML response")
    return r.json()

def hybrid_fetch(url):
    return fetch_cloud(url)

# =====================================================
# FETCH ONE VENUE (CORRECT DATE LOGIC)
# =====================================================
def fetch_venue(venue_code):
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )

    data = hybrid_fetch(url)
    sd = data.get("ShowDetails", [])
    if not sd:
        print(f"⚠️ [EMPTY] {venue_code} | no ShowDetails")
        return {}

    venue_info = sd[0].get("Venues", {})
    venue_name = venue_info.get("VenueName", "")
    venue_add  = venue_info.get("VenueAdd", "")
    chain      = venue_info.get("VenueCompName", "Unknown")

    out = defaultdict(list)
    valid_shows = 0

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

                valid_shows += 1
                out[movie].append({
                    "venue": venue_name,
                    "address": venue_add,
                    "chain": chain,
                    "time": sh.get("ShowTime"),
                    "session_id": sh.get("SessionId"),
                    "audi": sh.get("Attributes", ""),
                    "total": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2),
                })

    if valid_shows:
        print(f"✅ [FETCHED] {venue_code} | shows={valid_shows}")
    else:
        print(f"⚠️ [EMPTY] {venue_code} | no shows for DATE={DATE_CODE}")

    return out

# =====================================================
# AGGREGATION
# =====================================================
def aggregate(all_data, venues_meta):
    summary = {}
    detailed = []

    for vcode, movies in all_data.items():
        meta = venues_meta.get(vcode, {})
        city  = meta.get("City", "Unknown")
        state = meta.get("State", "Unknown")

        for movie, shows in movies.items():
            m = summary.setdefault(movie, {
                "shows": 0, "gross": 0, "sold": 0, "totalSeats": 0,
                "venues": set(), "cities": set(),
                "fastfilling": 0, "housefull": 0
            })

            m["venues"].add(vcode)
            m["cities"].add(city)

            for s in shows:
                sold = s["sold"]
                total = s["total"]
                occ = (sold / total * 100) if total else 0

                m["shows"] += 1
                m["gross"] += s["gross"]
                m["sold"] += sold
                m["totalSeats"] += total

                if occ >= 98:
                    m["housefull"] += 1
                elif occ >= 50:
                    m["fastfilling"] += 1

                detailed.append({
                    "movie": movie,
                    "city": city,
                    "state": state,
                    "venue": s["venue"],
                    "address": s["address"],
                    "time": s["time"],
                    "audi": s["audi"],
                    "session_id": s["session_id"],
                    "totalSeats": total,
                    "available": s["available"],
                    "sold": sold,
                    "gross": s["gross"],
                    "occupancy": round(occ, 2),
                    "source": "BMS",
                    "date": DATE_CODE
                })

    final = {
        k: {
            "shows": v["shows"],
            "gross": round(v["gross"], 2),
            "sold": v["sold"],
            "totalSeats": v["totalSeats"],
            "venues": len(v["venues"]),
            "cities": len(v["cities"]),
            "fastfilling": v["fastfilling"],
            "housefull": v["housefull"],
            "occupancy": round(v["sold"] / v["totalSeats"] * 100, 2) if v["totalSeats"] else 0
        }
        for k, v in summary.items()
    }

    return final, detailed

# =====================================================
# MEMORY DUMP
# =====================================================
def dump_memory(venues):
    summary, detailed = aggregate(all_data, venues)
    with open(SUMMARY_FILE, "w") as f:
        json.dump(summary, f, indent=2)
    with open(DETAILED_FILE, "w") as f:
        json.dump(detailed, f, indent=2)
    print(f"💾 Memory dump done at {fetch_count} venues")

# =====================================================
# FETCH WRAPPER
# =====================================================
def fetch_safe(vcode, venues):
    global fetch_count, error_count
    try:
        data = fetch_venue(vcode)
        with lock:
            all_data[vcode] = data
            if not data:
                empty_venues.add(vcode)
            fetch_count += 1
            if fetch_count % DUMP_EVERY == 0:
                dump_memory(venues)
    except Exception as e:
        with lock:
            error_count += 1
            print(f"❌ [FAILED] {vcode}: {e}")
            if error_count >= MAX_ERRORS:
                dump_memory(venues)
                sys.exit(1)

# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    with open("venues1.json", "r") as f:
        venues = json.load(f)

    print("🚀 FIRST PASS STARTED")

    with ThreadPoolExecutor(NUM_WORKERS) as exe:
        futures = [exe.submit(fetch_safe, v, venues) for v in venues.keys()]
        for _ in as_completed(futures):
            pass

    print(f"\n🔁 SECOND PASS — retrying {len(empty_venues)} empty venues\n")

    for vcode in list(empty_venues):
        time.sleep(random.uniform(1.5, 3.0))
        reset_identity()
        data = fetch_venue(vcode)
        if data:
            all_data[vcode] = data
            empty_venues.remove(vcode)
            print(f"✅ [RECOVERED] {vcode}")
        else:
            print(f"⚠️ [STILL EMPTY] {vcode}")

    dump_memory(venues)
    print("✅ DONE — bms1.py complete")
