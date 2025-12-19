import json
import os
import asyncio
import aiohttp
from collections import defaultdict
from datetime import datetime, timedelta
import pytz

# =====================================================
# CONFIG
# =====================================================
SHARD_ID = 9
CONCURRENCY = 20
TIMEOUT = aiohttp.ClientTimeout(total=25)

IST = pytz.timezone("Asia/Kolkata")
NOW_IST = datetime.now(IST)
DATE_CODE = (NOW_IST + timedelta(days=1)).strftime("%Y%m%d")
DATE_DISTRICT = (NOW_IST + timedelta(days=1)).strftime("%Y-%m-%d")

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
LOG_FILE      = f"{LOG_DIR}/districtbms{SHARD_ID}.log"

API_URL = "https://districtvenues.text2027mail.workers.dev/?cinema_id={cid}&date={date}"

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

def dedupe(rows):
    seen = set()
    out = []

    for r in rows:
        key = (
            r.get("venue", ""),
            r.get("time", ""),
            str(r.get("session_id", "")),
            r.get("audi", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out

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

            # ---- VENUE LEVEL DATE SKIP ----
            if DATE_DISTRICT not in session_dates:
                return None

            return {"venue": venue, "data": data}

    except Exception as e:
        log(f"❌ {cid} {type(e).__name__}")
        return None

# =====================================================
# MAIN ASYNC FETCH
# =====================================================
async def fetch_all():
    sem = asyncio.Semaphore(CONCURRENCY)
    results = []

    async with aiohttp.ClientSession(timeout=TIMEOUT) as session:

        async def bound_fetch(v):
            async with sem:
                return await fetch_one(session, v)

        tasks = [bound_fetch(v) for v in DIST_VENUES]
        raw = await asyncio.gather(*tasks)

    for r in raw:
        if r:
            results.append(r)

    log(f"✅ Fetched {len(results)} venues with shows")
    return results

# =====================================================
# PARSE DATA
# =====================================================
def parse(results):
    detailed = []
    summary = {}

    for res in results:
        venue_meta = res["venue"]
        data = res["data"]

        city = venue_meta.get("city", "Unknown")
        state = format_state(venue_meta.get("state"))
        chain = format_chain(venue_meta.get("chainKey"))

        cinema = data.get("meta", {}).get("cinema", {})
        venue_name = cinema.get("name", "")
        venue_addr = cinema.get("address", "")

        movies = data.get("meta", {}).get("movies", []) or []
        movie_map = {}
        for m in movies:
            movie_map[m.get("id")] = m
            movie_map[str(m.get("id"))] = m

        for s in data.get("pageData", {}).get("sessions", []) or []:
            mid = s.get("mid")
            movie = movie_map.get(mid) or movie_map.get(str(mid))
            if not movie:
                continue

            name = movie.get("name", "Unknown")
            lang = s.get("lang") or movie.get("lang") or ""
            fmt = s.get("scrnFmt") or ""
            fmt = fmt.replace("-", " | ") if fmt else ""

            movie_key = (
                f"{name} [{fmt} | {lang}]"
                if fmt else f"{name} | {lang}"
            )

            total = int(s.get("total", 0))
            avail = int(s.get("avail", 0))
            sold = total - avail
            gross = sum(
                (a.get("sTotal", 0) - a.get("sAvail", 0)) * a.get("price", 0)
                for a in s.get("areas", []) or []
            )
            occ = (sold / total * 100) if total else 0

            # ---------------- DETAILED ----------------
            detailed.append({
                "movie": movie_key,
                "city": city,
                "state": state,
                "venue": venue_name,
                "address": venue_addr,
                "time": (
                    datetime.strptime(s.get("showTime"), "%Y-%m-%dT%H:%M")
                    .replace(tzinfo=pytz.UTC)
                    .astimezone(IST)
                    .strftime("%I:%M %p")
                    if s.get("showTime") else ""
                ),
                "audi": s.get("audi", ""),
                "session_id": str(s.get("id", "")),
                "totalSeats": total,
                "available": avail,
                "sold": sold,
                "gross": round(gross, 2),
                "occupancy": f"{round(occ, 2)}%",
                "source": "District",
                "date": DATE_CODE,
                "chain": chain
            })

    detailed = dedupe(detailed)
    return detailed

# =====================================================
# BUILD SUMMARY (AFTER DEDUPE)
# =====================================================
def build_summary(detailed):
    summary = {}

    for r in detailed:
        movie = r["movie"]
        city = r["city"]
        state = r["state"]
        venue = r["venue"]
        chain = r["chain"]

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
                "housefull": 0,
                "details": {},
                "Chain_details": {}
            }

        m = summary[movie]
        m["shows"] += 1
        m["gross"] += gross
        m["sold"] += sold
        m["totalSeats"] += total
        m["venues"].add(venue)
        m["cities"].add(city)

        if occ >= 98: m["housefull"] += 1
        elif occ >= 50: m["fastfilling"] += 1

        ck = (city, state)
        if ck not in m["details"]:
            m["details"][ck] = {
                "city": city, "state": state,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }

        d = m["details"][ck]
        d["venues"].add(venue)
        d["shows"] += 1
        d["gross"] += gross
        d["sold"] += sold
        d["totalSeats"] += total
        if occ >= 98: d["housefull"] += 1
        elif occ >= 50: d["fastfilling"] += 1

        if chain not in m["Chain_details"]:
            m["Chain_details"][chain] = {
                "chain": chain,
                "venues": set(), "shows": 0,
                "gross": 0.0, "sold": 0,
                "totalSeats": 0,
                "fastfilling": 0, "housefull": 0
            }

        c = m["Chain_details"][chain]
        c["venues"].add(venue)
        c["shows"] += 1
        c["gross"] += gross
        c["sold"] += sold
        c["totalSeats"] += total
        if occ >= 98: c["housefull"] += 1
        elif occ >= 50: c["fastfilling"] += 1

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
            "occupancy": round((m["sold"] / m["totalSeats"]) * 100, 2) if m["totalSeats"] else 0.0,
            "details": [],
            "Chain_details": []
        }

        for d in m["details"].values():
            final[movie]["details"].append({
                "city": d["city"],
                "state": d["state"],
                "venues": len(d["venues"]),
                "shows": d["shows"],
                "gross": round(d["gross"], 2),
                "sold": d["sold"],
                "totalSeats": d["totalSeats"],
                "fastfilling": d["fastfilling"],
                "housefull": d["housefull"],
                "occupancy": round((d["sold"] / d["totalSeats"]) * 100, 2) if d["totalSeats"] else 0.0
            })

        for c in m["Chain_details"].values():
            final[movie]["Chain_details"].append({
                "chain": c["chain"],
                "venues": len(c["venues"]),
                "shows": c["shows"],
                "gross": round(c["gross"], 2),
                "sold": c["sold"],
                "totalSeats": c["totalSeats"],
                "fastfilling": c["fastfilling"],
                "housefull": c["housefull"],
                "occupancy": round((c["sold"] / c["totalSeats"]) * 100, 2) if c["totalSeats"] else 0.0
            })

    return final

# =====================================================
# ENTRY
# =====================================================
async def main():
    log("🚀 DISTRICT SCRIPT STARTED")
    results = await fetch_all()
    detailed = parse(results)
    summary = build_summary(detailed)

    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Shows={len(detailed)} | Movies={len(summary)}")

if __name__ == "__main__":
    asyncio.run(main())
