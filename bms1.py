# ===================== IMPORTS =====================
import json
import os
import random
import time
import threading
import signal
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import cloudscraper

# ===================== CONFIG =====================
SHARD_ID = 1
API_TIMEOUT = 12
HARD_TIMEOUT = 15
MAX_RECOVERY_ROUNDS = 10

IST = timezone(timedelta(hours=5, minutes=30))
DATE_CODE = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = os.path.join("advance", "data", DATE_CODE)
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

SUMMARY_FILE  = f"{BASE_DIR}/movie_summary{SHARD_ID}.json"
DETAILED_FILE = f"{BASE_DIR}/detailed{SHARD_ID}.json"
LOG_FILE      = f"{LOG_DIR}/bms{SHARD_ID}.log"

# ===================== LOGGING =====================
def log(msg):
    ts = datetime.now(IST).strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ===================== HARD TIMEOUT =====================
class TimeoutError(Exception): pass
def _timeout_handler(signum, frame): raise TimeoutError()
def hard_timeout(sec):
    def deco(fn):
        def wrap(*a, **k):
            signal.signal(signal.SIGALRM, _timeout_handler)
            signal.alarm(sec)
            try: return fn(*a, **k)
            finally: signal.alarm(0)
        return wrap
    return deco

# ===================== STATE =====================
thread_local = threading.local()
all_data = {}
retry_venues = set()

# ===================== USER AGENTS =====================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/119 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/118 Safari/537.36",
]

# ===================== IDENTITY =====================
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
            "X-Forwarded-For": self.ip,
        }

def get_identity():
    if not hasattr(thread_local, "identity"):
        log("🧠 New identity")
        thread_local.identity = Identity()
    return thread_local.identity

def reset_identity():
    if hasattr(thread_local, "identity"):
        del thread_local.identity
    log("🔄 Identity reset")

# ===================== FETCH =====================
@hard_timeout(HARD_TIMEOUT)
def fetch_api_raw(vcode):
    ident = get_identity()
    url = f"https://in.bookmyshow.com/api/v2/mobile/showtimes/byvenue?venueCode={vcode}&dateCode={DATE_CODE}"
    r = ident.scraper.get(url, headers=ident.headers(), timeout=API_TIMEOUT)
    if not r.text.strip().startswith("{"):
        raise RuntimeError("HTML / Block")
    return r.json()

# ===================== PARSE =====================
def parse_payload(data, vcode):
    if data.get("AllShowDatesDisabled"):
        return {}, "ALL_DISABLED"

    sd = data.get("ShowDetails", [])
    if not sd:
        return {}, "NO_SHOWS"

    if sd[0].get("Date") and str(sd[0]["Date"]) != DATE_CODE:
        return {}, "DATE_MISMATCH"

    venue = sd[0].get("Venues", {})
    vname = venue.get("VenueName", "")
    vaddr = venue.get("VenueAdd", "")
    vchain = venue.get("VenueCompName", "Unknown")

    out = defaultdict(list)

    for ev in sd[0].get("Event", []):
        title = ev.get("EventTitle", "Unknown")

        for ch in ev.get("ChildEvents", []):
            dim = ch.get("EventDimension", "").strip()
            lang = ch.get("EventLanguage", "").strip()
            suffix = " | ".join(x for x in (dim, lang) if x)
            movie = f"{title} [{suffix}]" if suffix else title

            for sh in ch.get("ShowTimes", []):
                if (sh.get("ShowDateCode") or "") != DATE_CODE:
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

                out[movie].append({
                    "venue": vname,
                    "address": vaddr,
                    "chain": vchain,
                    "time": sh.get("ShowTime"),
                    "session_id": str(sh.get("SessionId", "")),
                    "audi": sh.get("Attributes", ""),
                    "total": total,
                    "available": avail,
                    "sold": sold,
                    "gross": round(gross, 2),
                })

    return out, "OK" if out else "NO_SHOWS"

# ===================== MAIN =====================
if __name__ == "__main__":
    log("🚀 START")

    with open("venues1.json", "r", encoding="utf-8") as f:
        venues = json.load(f)

    # ---------- FETCH ----------
    for vcode in venues:
        try:
            raw = fetch_api_raw(vcode)
            parsed, status = parse_payload(raw, vcode)
            if status == "OK":
                all_data[vcode] = parsed
            elif status not in ("OK",):
                pass
        except Exception:
            retry_venues.add(vcode)
            reset_identity()

    # ===================== BUILD DETAILED (DEDUPED) =====================
    seen = set()
    detailed = []

    for vcode, movies in all_data.items():
        meta = venues.get(vcode, {})
        city = meta.get("City", "Unknown")
        state = meta.get("State", "Unknown")

        for movie, shows in movies.items():
            for s in shows:
                key = (s["venue"], s["time"], s["session_id"], s["audi"])
                if key in seen:
                    continue
                seen.add(key)

                occ = round((s["sold"] / s["total"] * 100), 2) if s["total"] else 0

                detailed.append({
                    "movie": movie,
                    "city": city,
                    "state": state,
                    "venue": s["venue"],
                    "address": s["address"],
                    "time": s["time"],
                    "audi": s["audi"],
                    "session_id": s["session_id"],
                    "totalSeats": s["total"],
                    "available": s["available"],
                    "sold": s["sold"],
                    "gross": s["gross"],
                    "occupancy": f"{occ}%",
                    "source": "BMS",
                    "date": DATE_CODE
                })

    # ===================== SUMMARY =====================
    summary = {}

    for row in detailed:
        m = summary.setdefault(row["movie"], {
            "shows": 0, "gross": 0.0, "sold": 0, "totalSeats": 0,
            "venues": set(), "cities": set(),
            "fastfilling": 0, "housefull": 0,
            "details": {}, "Chain_details": {}
        })

        occ = float(row["occupancy"].replace("%", ""))
        m["shows"] += 1
        m["gross"] += row["gross"]
        m["sold"] += row["sold"]
        m["totalSeats"] += row["totalSeats"]
        m["venues"].add(row["venue"])
        m["cities"].add(row["city"])

        if occ >= 98:
            m["housefull"] += 1
        elif occ >= 50:
            m["fastfilling"] += 1

        # ---- city ----
        ckey = (row["city"], row["state"])
        cd = m["details"].setdefault(ckey, {
            "city": row["city"], "state": row["state"],
            "venues": set(), "shows": 0,
            "gross": 0.0, "sold": 0, "totalSeats": 0,
            "fastfilling": 0, "housefull": 0
        })
        cd["venues"].add(row["venue"])
        cd["shows"] += 1
        cd["gross"] += row["gross"]
        cd["sold"] += row["sold"]
        cd["totalSeats"] += row["totalSeats"]
        if occ >= 98: cd["housefull"] += 1
        elif occ >= 50: cd["fastfilling"] += 1

        # ---- chain ----
        chain = venues.get(vcode, {}).get("VenueCompName", "Unknown")
        ch = m["Chain_details"].setdefault(chain, {
            "chain": chain, "venues": set(),
            "shows": 0, "gross": 0.0,
            "sold": 0, "totalSeats": 0,
            "fastfilling": 0, "housefull": 0
        })
        ch["venues"].add(row["venue"])
        ch["shows"] += 1
        ch["gross"] += row["gross"]
        ch["sold"] += row["sold"]
        ch["totalSeats"] += row["totalSeats"]
        if occ >= 98: ch["housefull"] += 1
        elif occ >= 50: ch["fastfilling"] += 1

    # ---- finalize ----
    final_summary = {}
    for movie, v in summary.items():
        final_summary[movie] = {
            "shows": v["shows"],
            "gross": round(v["gross"], 2),
            "sold": v["sold"],
            "totalSeats": v["totalSeats"],
            "venues": len(v["venues"]),
            "cities": len(v["cities"]),
            "fastfilling": v["fastfilling"],
            "housefull": v["housefull"],
            "occupancy": round(v["sold"] / v["totalSeats"] * 100, 2) if v["totalSeats"] else 0,
            "details": [
                {
                    **d,
                    "venues": len(d["venues"]),
                    "occupancy": round(d["sold"] / d["totalSeats"] * 100, 2) if d["totalSeats"] else 0
                } for d in v["details"].values()
            ],
            "Chain_details": [
                {
                    **c,
                    "venues": len(c["venues"]),
                    "occupancy": round(c["sold"] / c["totalSeats"] * 100, 2) if c["totalSeats"] else 0
                } for c in v["Chain_details"].values()
            ]
        }

    # ===================== SAVE =====================
    with open(DETAILED_FILE, "w", encoding="utf-8") as f:
        json.dump(detailed, f, indent=2, ensure_ascii=False)

    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(final_summary, f, indent=2, ensure_ascii=False)

    log(f"✅ DONE | Detailed={len(detailed)} | Movies={len(final_summary)}")
