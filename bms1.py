import json
import os
import sys
import time
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import cloudscraper

# ---------------- SELENIUM ----------------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ----------------
SHARD_ID = 1                     # 🔁 CHANGE FOR bms2.py … bms7.py
NUM_WORKERS = 3
MAX_ERRORS = 20
MAX_RETRY_CLOUD = 2

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

ADVANCE_DIR = f"advance/{DATE_CODE}"
os.makedirs(ADVANCE_DIR, exist_ok=True)

DETAILED_OUT = f"{ADVANCE_DIR}/detailed_{SHARD_ID}.json"
SUMMARY_OUT  = f"{ADVANCE_DIR}/summary_{SHARD_ID}.json"
FETCHED_FILE = f"{ADVANCE_DIR}/fetchedvenues_{SHARD_ID}.json"
FAILED_FILE  = f"{ADVANCE_DIR}/failedvenues_{SHARD_ID}.json"

lock = threading.Lock()
error_count = 0
thread_local = threading.local()

# ---------------- USER AGENTS ----------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

def random_ip():
    return ".".join(str(random.randint(10, 240)) for _ in range(4))

def get_headers():
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

# ---------------- LOAD STATE ----------------
def load_set(path):
    if os.path.exists(path):
        with open(path) as f:
            return set(json.load(f))
    return set()

fetched_venues = load_set(FETCHED_FILE)
failed_venues = load_set(FAILED_FILE)

all_data = {}   # venue_code → parsed shows

# ---------------- SAVE STATE ----------------
def dump_state():
    with open(FETCHED_FILE, "w") as f:
        json.dump(list(fetched_venues), f)
    with open(FAILED_FILE, "w") as f:
        json.dump(list(failed_venues), f)

# ---------------- CLOUDSCRAPER ----------------
def fetch_cloudscraper(url):
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    res = scraper.get(url, headers=get_headers(), timeout=15)
    text = res.text.strip()
    if not text.startswith("{"):
        raise ValueError("Cloudflare / HTML")
    return res.json()

# ---------------- SELENIUM ----------------
def get_driver():
    if hasattr(thread_local, "driver"):
        return thread_local.driver

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    thread_local.driver = driver
    return driver

def fetch_selenium(url):
    driver = get_driver()
    driver.get(url)
    body = driver.page_source.strip()
    if not body.startswith("{"):
        raise ValueError("Selenium HTML")
    return json.loads(body)

# ---------------- HYBRID ----------------
def hybrid_fetch(url):
    for _ in range(MAX_RETRY_CLOUD):
        try:
            return fetch_cloudscraper(url)
        except:
            time.sleep(1)
    return fetch_selenium(url)

# ---------------- FETCH DATA ----------------
def fetch_data(venue_code):
    url = (
        "https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue"
        f"?venueCode={venue_code}&dateCode={DATE_CODE}"
    )
    data = hybrid_fetch(url)

    sd = data.get("ShowDetails", [])
    if not sd:
        return {}

    venue = sd[0].get("Venues", {})
    venue_name = venue.get("VenueName", "")
    venue_add  = venue.get("VenueAdd", "")
    chain = venue.get("VenueCompName", "Unknown")

    shows_by_movie = defaultdict(list)

    for event in sd[0].get("Event", []):
        title = event.get("EventTitle", "Unknown")
        for child in event.get("ChildEvents", []):
            dim = child.get("EventDimension", "").strip()
            lang = child.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for show in child.get("ShowTimes", []):
                total = sold = gross = 0
                for cat in show.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    avail = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    sold += seats - avail
                    gross += (seats - avail) * price

                shows_by_movie[movie].append({
                    "venue": venue_name,
                    "address": venue_add,
                    "chain": chain,
                    "movie": movie,
                    "time": show.get("ShowTime"),
                    "total": total,
                    "sold": sold,
                    "gross": gross,
                })
    return shows_by_movie

# ---------------- AGGREGATE (MainApi style) ----------------
def aggregate(all_data, venues_meta):
    summary = {}
    detailed = []

    for vcode, movies in all_data.items():
        meta = venues_meta.get(vcode, {})
        city = meta.get("City", "Unknown")
        state = meta.get("State", "Unknown")

        for movie, shows in movies.items():
            if movie not in summary:
                summary[movie] = {
                    "shows": 0, "gross": 0, "sold": 0,
                    "totalSeats": 0, "venues": 0,
                    "cities": set(), "fastfilling": 0, "housefull": 0
                }

            summary[movie]["venues"] += 1
            summary[movie]["cities"].add(city)

            for s in shows:
                occ = (s["sold"] / s["total"] * 100) if s["total"] else 0
                summary[movie]["shows"] += 1
                summary[movie]["gross"] += s["gross"]
                summary[movie]["sold"] += s["sold"]
                summary[movie]["totalSeats"] += s["total"]
                if occ >= 98:
                    summary[movie]["housefull"] += 1
                elif occ >= 50:
                    summary[movie]["fastfilling"] += 1

                detailed.append({
                    "movie": movie,
                    "city": city,
                    "state": state,
                    "venue": s["venue"],
                    "address": s["address"],
                    "time": s["time"],
                    "totalSeats": s["total"],
                    "sold": s["sold"],
                    "gross": s["gross"],
                    "occupancy": round(occ, 2),
                    "date": DATE_CODE,
                })

    for v in summary.values():
        v["cities"] = len(v["cities"])
        v["occupancy"] = round(
            (v["sold"] / v["totalSeats"] * 100), 2
        ) if v["totalSeats"] else 0

    return summary, detailed

# ---------------- SAFE FETCH ----------------
def fetch_venue_safe(code):
    global error_count
    with lock:
        if code in fetched_venues or code in failed_venues:
            return

    try:
        data = fetch_data(code)
    except Exception:
        with lock:
            failed_venues.add(code)
            error_count += 1
        return

    with lock:
        all_data[code] = data
        fetched_venues.add(code)
        print(f"✅ {code} ({len(fetched_venues)})")

# ---------------- MAIN ----------------
if __name__ == "__main__":
    with open(f"venues{SHARD_ID}.json") as f:
        venues = json.load(f)

    with open("venues.json") as f:
        venues_meta = json.load(f)

    print(f"🚀 Shard {SHARD_ID} start | venues={len(venues)}")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as exe:
        futures = [exe.submit(fetch_venue_safe, v) for v in venues.keys()]
        for _ in as_completed(futures):
            pass

    dump_state()

    final, detailed = aggregate(all_data, venues_meta)

    with open(DETAILED_OUT, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_OUT, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)

    print(f"✅ DONE shard {SHARD_ID}")
