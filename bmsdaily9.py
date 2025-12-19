import json
import os
import asyncio
import aiohttp
from datetime import datetime, timedelta
import pytz

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 9
CONCURRENCY = 20
TIMEOUT = aiohttp.ClientTimeout(total=25)

CUTOFF_MINUTES = 200   # ⏱️ same as BMS daily

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)

# 🔥 TODAY ONLY
DATE_CODE = NOW_IST.strftime("%Y%m%d")
DATE_DISTRICT = NOW_IST.strftime("%Y-%m-%d")

BASE_DIR = os.path.join("daily", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
LOG_FILE      = f"{LOG_DIR}/districtdaily{SHARD_ID}.log"

API_URL = "https://districtvenues.text2029mail.workers.dev/?cinema_id={cid}&date={date}"

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
# LOAD DISTRICT VENUES
# =====================================================
with open("districtvenues.json", "r", encoding="utf-8") as f:
    DIST_VENUES = json.load(f)

log(f"📍 Loaded {len(DIST_VENUES)} district venues")

# =====================================================
# HELPERS
# =====================================================
def format_state(s):
    if not s:
        return "Unknown"
    return " ".join(w.capitalize() for w in s.replace("-", " ").split())

def format_chain(s):
    if not s:
        return "Unknown"
    return " ".join(w.capitalize() for w in s.replace("-", " ").split())

def minutes_left(show_time_str):
    """
    Calculate minutes left from now (IST).
    Allows negative values (running / past shows).
    Handles post-midnight rollover.
    """
    try:
        now = NOW_IST
        t = datetime.strptime(show_time_str, "%I:%M %p").replace(
            year=now.year,
            month=now.month,
            day=now.day,
            tzinfo=IST
        )

        # handle post-midnight shows
        if t < now - timedelta(hours=6):
            t += timedelta(days=1)

        return (t - now).total_seconds() / 60
    except:
        return 9999

def show_key(r):
    return (
        r.get("venue"),
        r.get("time"),
        r.get("session_id"),
        r.get("audi"),
    )

# =====================================================
# FETCH SINGLE VENUE
# =====================================================
async def fetch_one(session, venue):
    cid = venue.get("id")
    url = API_URL.format(cid=cid, date=DATE_DISTRICT)

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                log(f"⚠ {cid} status {resp.status}")
                return None

            data = await resp.json()
            session_dates = data.get("data", {}).get("sessionDates", [])

            if DATE_DISTRICT not in session_dates:
                return None

            return {"venue": venue, "data": data}

    except Exception as e:
        log(f"❌ {cid} {type(e).__name__}")
        return None

# =====================================================
# FETCH ALL (ASYNC)
# =====================================================
async def fetch_all():
    sem = asyncio.Semaphore(CONCURRENCY)
    results = []

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:

        async def bound(v):
            async with sem:
                return await fetch_one(session, v)

        tasks = [bound(v) for v in DIST_VENUES]
        raw = await asyncio.gather(*tasks)

    for r in raw:
        if r:
            results.append(r)

    log(f"✅ Fetched {len(results)} venues with shows")
    return results

# =====================================================
# PARSE + APPLY CUTOFF
# =====================================================
def parse(results):
    detailed = []

    for res in results:
        venue_meta = res["venue"]
        data = res["data"]

        city = venue_meta.get("city") or "Unknown"
        state = format_state(venue_meta.get("state"))

        cinema = data.get("meta", {}).get("cinema", {})

        venue_name = (
            cinema.get("name")
            or venue_meta.get("name")
            or venue_meta.get("district_name")
            or "Unknown"
        )

        venue_addr = cinema.get("address") or venue_meta.get("address") or ""

        chain = format_chain(
            venue_meta.get("chainKey")
            or venue_meta.get("chain")
            or venue_name
        )

        movies = data.get("meta", {}).get("movies", []) or []
        movie_map = {str(m.get("id")): m for m in movies}

        for s in data.get("pageData", {}).get("sessions", []) or []:
            movie = movie_map.get(str(s.get("mid")))
            if not movie:
                continue

            name = movie.get("name", "Unknown")
            lang = s.get("lang") or movie.get("lang") or ""
            fmt = (s.get("scrnFmt") or "").replace("-", " | ")

            movie_key = f"{name} [{fmt} | {lang}]" if fmt else f"{name} | {lang}"

            show_time = (
                datetime.strptime(s.get("showTime"), "%Y-%m-%dT%H:%M")
                .replace(tzinfo=pytz.UTC)
                .astimezone(IST)
                .strftime("%I:%M %p")
            )

            mins = minutes_left(show_time)
            if mins > CUTOFF_MINUTES:
                continue

            total = int(s.get("total", 0))
            avail = int(s.get("avail", 0))
            sold = total - avail

            gross = sum(
                (a.get("sTotal", 0) - a.get("sAvail", 0)) * a.get("price", 0)
                for a in s.get("areas", []) or []
            )

            detailed.append({
                "movie": movie_key,
                "city": city,
                "state": state,
                "venue": venue_name,
                "address": venue_addr,
                "time": show_time,
                "audi": s.get("audi", ""),
                "session_id": str(s.get("id", "")),
                "totalSeats": total,
                "available": avail,
                "sold": sold,
                "gross": round(gross, 2),
                "minsLeft": round(mins, 1),
                "source": "District",
                "date": DATE_CODE,
                "chain": chain
            })

    return detailed

# =====================================================
# BUILD SUMMARY (FROM FINAL DETAILED)
# =====================================================
def build_summary(detailed):
    summary = {}

    for r in detailed:
        movie = r["movie"]
        city = r["city"]
        venue = r["venue"]

        total = r["totalSeats"]
        sold = r["sold"]
        gross = r["gross"]
        occ = (sold / total * 100) if total else 0

        if movie not in summary:
            summary[movie] = {
                "shows": 0,
                "gross": 0.0,
                "sold": 0,
                "totalSeats": 0,
                "venues": set(),
                "cities": set(),
                "fastfilling": 0,
                "housefull": 0
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)

        if occ >= 98:
            m["housefull"] += 1
        elif occ >= 50:
            m["fastfilling"] += 1

    return {
        movie: {
            "shows": m["shows"],
            "gross": round(m["gross"], 2),
            "sold": m["sold"],
            "totalSeats": m["totalSeats"],
            "venues": len(m["venues"]),
            "cities": len(m["cities"]),
            "fastfilling": m["fastfilling"],
            "housefull": m["housefull"],
            "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0
        }
        for movie, m in summary.items()
    }

# =====================================================
# ENTRY
# =====================================================
async def main():
    log("🚀 DISTRICT DAILY SCRAPER STARTED")

    results = await fetch_all()
    fresh = parse(results)

    # -------- NEVER DELETE SHOWS --------
    if os.path.exists(DETAILED_FILE):
        with open(DETAILED_FILE, "r", encoding="utf-8") as f:
            old_rows = json.load(f)
    else:
        old_rows = []

    old_map = {show_key(r): r for r in old_rows}
    new_map = {}

    for r in fresh:
        key = show_key(r)
        if key in old_map:
            old_map[key].update({
                "totalSeats": r["totalSeats"],
                "available": r["available"],
                "sold": r["sold"],
                "gross": r["gross"],
                "minsLeft": r["minsLeft"]
            })
            new_map[key] = old_map[key]
        else:
            new_map[key] = r

    for key, r in old_map.items():
        if key not in new_map:
            new_map[key] = r

    detailed = list(new_map.values())
    summary = build_summary(detailed)

    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)}")

if __name__ == "__main__":
    asyncio.run(main())
