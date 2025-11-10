import os
import json
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

@app.get("/")
async def root():
    return {"status": "ok", "app": "data-bridge"}