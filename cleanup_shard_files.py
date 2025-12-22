import os
from datetime import datetime, timedelta
import pytz

# ================= CONFIG =================
START_DATE = "20251219"
BASE_PATHS = [
    "advance/data",
    "daily/data"
]

IST = pytz.timezone("Asia/Kolkata")
YESTERDAY_IST = (datetime.now(IST) - timedelta(days=1)).strftime("%Y%m%d")

FILES_TO_DELETE = [
    *(f"detailed{i}.json" for i in range(1, 10)),
    *(f"movie_summary{i}.json" for i in range(1, 10)),
]

# ================= HELPERS =================
def daterange(start, end):
    cur = datetime.strptime(start, "%Y%m%d")
    end = datetime.strptime(end, "%Y%m%d")
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)

# ================= CLEANUP =================
deleted = 0

for date in daterange(START_DATE, YESTERDAY_IST):
    for base in BASE_PATHS:
        folder = os.path.join(base, date)
        if not os.path.isdir(folder):
            continue

        for fname in FILES_TO_DELETE:
            path = os.path.join(folder, fname)
            if os.path.exists(path):
                os.remove(path)
                deleted += 1
                print(f"🗑 Deleted: {path}")

print(f"\n✅ Cleanup complete. Files removed: {deleted}")
