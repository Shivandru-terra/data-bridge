import os
from app.services.mixpanel import mixPanelServices
from app.db.sqlite_store import SqliteStore
from app.services.gcp_bucket import gcp_Bucket_Services
from app.utils.general_functions import generalFunctions
import time

def run_pipeline(day_str: str):
    sqlite_path = SqliteStore.db_path_for_date(day_str)
    sqlite_store = SqliteStore(sqlite_path)
    CHANNEL = "shivandru-self-dm"
    try:
        print(f"🚀 Starting pipeline for {day_str}")
        sqlite_store.init_db()
        batch, batch_size = [], 5000
        print(f"📡 Fetching Mixpanel events for {day_str} ...")
        for event in mixPanelServices.fetch_stream(day_str, day_str, slack_channel=CHANNEL):
            batch.append(event)
            if len(batch) >= batch_size:
                sqlite_store.insert_batch(batch)
                batch.clear()
        if batch:
            sqlite_store.insert_batch(batch)

        total = sqlite_store.count_events()
        print(f"✅ {total} deduped events for {day_str}")

        gcp_Bucket_Services.upload_day(sqlite_path)
        print(f"✅ Uploaded {day_str}")

        sqlite_store.close()
        time.sleep(1)

        os.remove(sqlite_path)
        print(f"🧹 Cleaned up {sqlite_path}")

    except Exception as e:
        generalFunctions.slack_service(CHANNEL, f"Upload failed: {e}", day_str, status="FAILURE")
        raise
