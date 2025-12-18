import json
import os
import sys
import time
import threading
import random
import atexit
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
MAX_RETRY_CLOUD = 2
DUMP_EVERY = 25
RAW_OUT = f"outputs/raw_1.json"   # for bms1.py
IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = DATE_CODE
os.makedirs(BASE_DIR, exist_ok=True)

DATA_FILE = f"{BASE_DIR}/venues_data.json"
FETCHED_FILE = f"{BASE_DIR}/fetchedvenues.json"
FAILED_FILE = f"{BASE_DIR}/failedvenues.json"

lock = threading.Lock()
thread_local = threading.local()
error_count = 0
processed_since_dump = 0

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
# LOAD / SAVE STATE
# ==========================================================
def load_set(path):
    if os.path.exists(path):
        with open(path) as f:
            return set(json.load(f))
    return set()

fetched_venues = load_set(FETCHED_FILE)
failed_venues = load_set(FAILED_FILE)

if os.path.exists(DATA_FILE):
    with open(DATA_FILE) as f:
        all_data = json.load(f)
else:
    all_data = {}

def dump_progress():
    with open(DATA_FILE, "w") as f:
        json.dump(all_data, f)

    with open(FETCHED_FILE, "w") as f:
        json.dump(sorted(fetched_venues), f)

    with open(FAILED_FILE, "w") as f:
        json.dump(sorted(failed_venues), f)

    print(f"💾 Saved | fetched={len(fetched_venues)} failed={len(failed_venues)}")

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
    for i in range(MAX_RETRY_CLOUD):
        try:
            r = scraper.get(url, headers=headers(), timeout=15)
            if not r.text.strip().startswith("{"):
                raise ValueError("HTML")
            return r.json()
        except Exception:
            if i + 1 == MAX_RETRY_CLOUD:
                raise
            time.sleep(2 + random.random())

# ==========================================================
# SELENIUM
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

def close_driver():
    if hasattr(thread_local, "driver"):
        try:
            thread_local.driver.quit()
        except:
            pass

atexit.register(close_driver)

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

    try:
        data = hybrid_fetch(url)
    except Exception:
        return None

    sd = data.get("ShowDetails")
    if not isinstance(sd, list) or not sd:
        return {}

    venue_info = sd[0].get("Venues", {})
    venue_name = venue_info.get("VenueName", "")
    venue_add = venue_info.get("VenueAdd", "")
    chain = venue_info.get("VenueCompName", "Unknown")

    out = defaultdict(list)

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")
        for ch in ev.get("ChildEvents", []):
            dim = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                total = sold = avail = gross = 0
                for cat in sh.get("Categories", []):
                    seats = int(cat.get("MaxSeats", 0))
                    free = int(cat.get("SeatsAvail", 0))
                    price = float(cat.get("CurPrice", 0))
                    total += seats
                    avail += free
                    sold += seats - free
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
# SAFE FETCH
# ==========================================================
def fetch_venue_safe(v):
    global error_count, processed_since_dump

    with lock:
        if v in fetched_venues or v in failed_venues:
            return

    data = fetch_data(v)

    with lock:
        if data is None:
            failed_venues.add(v)
            error_count += 1
            print(f"❌ {v} failed")
        else:
            all_data[v] = data
            fetched_venues.add(v)
            processed_since_dump += 1
            print(f"✅ {v} fetched ({len(fetched_venues)})")

        if processed_since_dump >= DUMP_EVERY:
            dump_progress()
            processed_since_dump = 0

        if error_count >= MAX_ERRORS:
            dump_progress()
            sys.exit(1)

# ==========================================================
# AGGREGATION (MATCHES MainApi.py)
# ==========================================================
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
                    "shows": 0,
                    "gross": 0,
                    "sold": 0,
                    "totalSeats": 0,
                    "venues": set(),
                    "cities": set(),
                    "fastfilling": 0,
                    "housefull": 0,
                    "details": {},
                    "Chain_details": {}
                }

            m = summary[movie]
            m["venues"].add(vcode)
            m["cities"].add(city)

            for s in shows:
                sold = s["sold"]
                total = s["total"]
                gross = s["gross"]
                occ = (sold / total * 100) if total else 0

                m["shows"] += 1
                m["gross"] += gross
                m["sold"] += sold
                m["totalSeats"] += total

                if occ >= 98:
                    m["housefull"] += 1
                elif occ >= 50:
                    m["fastfilling"] += 1

                ck = f"{city}|{state}"
                if ck not in m["details"]:
                    m["details"][ck] = {
                        "city": city,
                        "state": state,
                        "venues": set(),
                        "shows": 0,
                        "gross": 0,
                        "sold": 0,
                        "totalSeats": 0,
                        "fastfilling": 0,
                        "housefull": 0
                    }

                d = m["details"][ck]
                d["venues"].add(vcode)
                d["shows"] += 1
                d["gross"] += gross
                d["sold"] += sold
                d["totalSeats"] += total

                if occ >= 98:
                    d["housefull"] += 1
                elif occ >= 50:
                    d["fastfilling"] += 1

                chain = s.get("chain", "Unknown")
                if chain not in m["Chain_details"]:
                    m["Chain_details"][chain] = {
                        "chain": chain,
                        "venues": set(),
                        "shows": 0,
                        "gross": 0,
                        "sold": 0,
                        "totalSeats": 0,
                        "fastfilling": 0,
                        "housefull": 0
                    }

                c = m["Chain_details"][chain]
                c["venues"].add(vcode)
                c["shows"] += 1
                c["gross"] += gross
                c["sold"] += sold
                c["totalSeats"] += total

                if occ >= 98:
                    c["housefull"] += 1
                elif occ >= 50:
                    c["fastfilling"] += 1

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
                    "occupancy": f"{round(occ,2)}%",
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
            "details": [
                {
                    **d,
                    "venues": len(d["venues"]),
                    "occupancy": round((d["sold"] / d["totalSeats"] * 100), 2)
                    if d["totalSeats"] else 0
                } for d in m["details"].values()
            ],
            "Chain_details": [
                {
                    **c,
                    "venues": len(c["venues"]),
                    "occupancy": round((c["sold"] / c["totalSeats"] * 100), 2)
                    if c["totalSeats"] else 0
                } for c in m["Chain_details"].values()
            ]
        }

    return final, detailed

# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    with open("venues1.json") as f:
        venues = json.load(f)

    print(f"🚀 Hybrid start | workers={NUM_WORKERS}")

    with ThreadPoolExecutor(NUM_WORKERS) as exe:
        futures = [exe.submit(fetch_venue_safe, v) for v in venues.keys()]
        for _ in as_completed(futures):
            pass

    dump_progress()

    with open(RAW_OUT, "w") as f:
        json.dump(all_data, f)

    print("✅ RAW scrape done for shard 1")
    print("✅ DONE — summary & detailed generated")
