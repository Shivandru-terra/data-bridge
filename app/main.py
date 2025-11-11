import os
import json
from fastapi import FastAPI
import asyncio
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, timedelta
from app.jobs.run_pipeline import run_pipeline
from app.utils.general_functions import generalFunctions

app = FastAPI(title="Data-Bridge", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CHANNEL = "shivandru-self-dm"
PROGRESS_FILE = "tmp_sqlite/progress.json"

# -------- Progress Tracker Utilities --------
def load_progress():
    if not os.path.exists(PROGRESS_FILE):
        return {"last_completed": None}
    with open(PROGRESS_FILE, "r") as f:
        return json.load(f)

def save_progress(last_completed: str):
    os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_completed": last_completed}, f)

def daterange(start_date: date, end_date: date):
    """Yield all dates between start_date and end_date inclusive."""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def run_backfill_sync():
    start_date = date(2024, 8, 8)
    end_date = date(2024, 8, 15)
    # end_date = date(2025, 10, 31)

    progress = load_progress()
    last_completed = progress.get("last_completed")

    print(f"🚀 Starting backfill from {start_date} → {end_date}")
    print(f"📘 Last completed: {last_completed}")

    start_flag = False if last_completed else True

    for current_day in daterange(start_date, end_date):
        day_str = current_day.strftime("%Y-%m-%d")

        if not start_flag:
            if day_str == last_completed:
                start_flag = True  # resume from next day
            continue

        print(f"\n📅 Processing {day_str} ...")
        try:
            run_pipeline(day_str)
            save_progress(day_str)
            print(f"✅ Completed {day_str}")
        except Exception as e:
            error_msg = f"❌ Failed pipeline for {day_str}: {str(e)}"
            print(error_msg)
            generalFunctions.slack_service(CHANNEL, error_msg)
            continue
    generalFunctions.slack_service(CHANNEL, f"🎯 Backfill complete up to {end_date}")
    print("✅ Backfill complete!")

async def run_backfill_background():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_backfill_sync)

@app.on_event("startup")
async def startup_event():
    """Triggered automatically when FastAPI starts."""
    asyncio.create_task(run_backfill_background())

@app.get("/")
async def root():
    return {"status": "ok", "app": "data-bridge"}