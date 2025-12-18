import json
import os
from datetime import datetime, timedelta
import pytz

# ---------------- IST DATE + 1 ----------------
IST = pytz.timezone("Asia/Kolkata")
date_ist_plus_1 = (datetime.now(IST) + timedelta(days=1)).strftime("%Y%m%d")

BASE_DIR = f"data/{date_ist_plus_1}"

print(f"📁 Using directory: {BASE_DIR}")

# ---------------- HELPERS ----------------
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️ Missing: {path}")
        return None
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON: {path}")
        return None


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------- COMBINE DETAILED ----------------
final_detailed = {}

for i in range(1, 9):
    file_path = os.path.join(BASE_DIR, f"detailed{i}.json")
    data = load_json(file_path)

    if isinstance(data, dict):
        final_detailed.update(data)

print(f"✅ Combined detailed files: {len(final_detailed)} entries")

save_json(os.path.join(BASE_DIR, "finaldetailed.json"), final_detailed)


# ---------------- COMBINE MOVIE SUMMARY ----------------
final_movie_summary = {}

for i in range(1, 9):
    file_path = os.path.join(BASE_DIR, f"movie_summary{i}.json")
    data = load_json(file_path)

    if isinstance(data, dict):
        final_movie_summary.update(data)

print(f"✅ Combined movie summary files: {len(final_movie_summary)} entries")

save_json(
    os.path.join(BASE_DIR, "finalmovie_summary.json"),
    final_movie_summary
)

print("🎉 All shard files combined successfully")
