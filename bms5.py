import json
import os
import sys
import time
import threading
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import cloudscraper

# ---------- SELENIUM ----------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ==========================================================
# CONFIG
# ==========================================================
NUM_WORKERS = 3
MAX_ERRORS = 20
SHARD_ID = 5

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
os.makedirs(BASE_DIR, exist_ok=True)

SUMMARY_FILE = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"

lock = threading.Lock()
thread_local = threading.local()
error_count = 0

# ==========================================================
# USER AGENTS
# ==========================================================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
]

def random_ip():
    return ".".join(str(random.randint(10, 240)) for _ in range(4))

def headers():
    ip = random_ip()
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://in.bookmyshow.com",
        "Referer": "https://in.bookmyshow.com/",
        "X-Forwarded-For": ip,
        "X-Real-IP": ip,
        "Client-IP": ip,
    }

# ==========================================================
# CLOUDSCRAPER
# ==========================================================
def get_scraper():
    if hasattr(thread_local, "scraper"):
        return thread_local.scraper
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    thread_local.scraper = s
    return s

def fetch_cloud(url):
    scraper = get_scraper()
    r = scraper.get(url, headers=headers(), timeout=15)
    if not r.text.strip().startswith("{"):
        raise ValueError("HTML")
    return r.json()

# ==========================================================
# SELENIUM FALLBACK
# ==========================================================
def get_driver():
    if hasattr(thread_local, "driver"):
        return thread_local.driver

    o = Options()
    o.add_argument("--headless=new")
    o.add_argument("--no-sandbox")
    o.add_argument("--disable-dev-shm-usage")
    o.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")

    d = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=o,
    )
    thread_local.driver = d
    return d

def fetch_selenium(url):
    d = get_driver()
    d.set_page_load_timeout(25)
    d.get(url)
    body = d.page_source.strip()
    if not body.startswith("{"):
        raise ValueError("HTML")
    return json.loads(body)

def hybrid_fetch(url):
    try:
        return fetch_cloud(url)
    except Exception:
        print("⚠️ Cloudflare → Selenium fallback")
        return fetch_selenium(url)

# ==========================================================
# FETCH VENUE DATA
# ==========================================================
def fetch_data(venue_code):
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )

    data = hybrid_fetch(url)

    sd = data.get("ShowDetails")
    if not isinstance(sd, list) or not sd:
        return {}

    venue_info = sd[0].get("Venues", {})
    venue_name = venue_info.get("VenueName", "")
    venue_add  = venue_info.get("VenueAdd", "")
    chain      = venue_info.get("VenueCompName", "Unknown")

    out = defaultdict(list)

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim  = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                total = sold = avail = gross = 0

                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free  = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold  += seats - free
                    gross += (seats - free) * price

                out[movie].append({
                    "venue": venue_name,
                    "address": venue_add,
                    "chain": chain,
                    "time": sh.get("ShowTime"),
                    "total": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2),
                })

    return out

# ==========================================================
# THREAD SAFE FETCH
# ==========================================================
all_data = {}

def fetch_venue_safe(v):
    global error_count

    try:
        data = fetch_data(v)
        with lock:
            all_data[v] = data
            print(f"✅ {v} fetched")
    except Exception:
        with lock:
            error_count += 1
            print(f"❌ {v} failed")
            if error_count >= MAX_ERRORS:
                sys.exit(1)

# ==========================================================
# AGGREGATION
# ==========================================================
def aggregate(all_data, venues_meta):
    summary = {}
    detailed = []

    for vcode, movies in all_data.items():
        meta = venues_meta.get(vcode, {})
        city  = meta.get("City", "Unknown")
        state = meta.get("State", "Unknown")

        for movie, shows in movies.items():
            if movie not in summary:
                summary[movie] = {
                    "shows": 0,
                    "gross": 0,
                    "sold": 0,
                    "totalSeats": 0,
                    "venues": set(),
                    "cities": set(),
                    "fastfilling": 0,
                    "housefull": 0,
                }

            m = summary[movie]
            m["venues"].add(vcode)
            m["cities"].add(city)

            for s in shows:
                sold  = s["sold"]
                total = s["total"]
                gross = s["gross"]
                occ   = (sold / total * 100) if total else 0

                m["shows"] += 1
                m["gross"] += gross
                m["sold"]  += sold
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
                    "totalSeats": total,
                    "available": s["available"],
                    "sold": sold,
                    "gross": gross,
                    "occupancy": round(occ, 2),
                    "source": "BMS",
                    "date": DATE_CODE
                })

    final = {}
    for movie, m in summary.items():
        final[movie] = {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"] * 100), 2)
            if m["totalSeats"] else 0,
        }

    return final, detailed

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    with open(f"venues{SHARD_ID}.json") as f:
        venues = json.load(f)

    print(f"🚀 Start | workers={NUM_WORKERS}")

    with ThreadPoolExecutor(NUM_WORKERS) as exe:
        futures = [exe.submit(fetch_venue_safe, v) for v in venues.keys()]
        for _ in as_completed(futures):
            pass

    movie_summary, detailed = aggregate(all_data, venues)

    # 🔥 OVERWRITE EVERY TIME
    with open(SUMMARY_FILE, "w") as f:
        json.dump(movie_summary, f, indent=2)

    with open(DETAILED_FILE, "w") as f:
        json.dump(detailed, f, indent=2)

    print("✅ DONE — summary.json & detailed.json OVERWRITTEN")
