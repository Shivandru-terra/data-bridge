import os
import sqlite3
from dotenv import load_dotenv
from google.cloud import storage
import tempfile

load_dotenv()
db = os.getenv("BUCKET_NAME")

class Gcp_Bucket_Services:
    CHUNK_SIZE_MB = 200
    CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)

    def yield_rows(self, db_path: str):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        for (raw_json,) in cur.execute("SELECT raw_json FROM mixpanel_events;"):
            yield raw_json
        conn.close()
    
    def upload_day(self, db_path: str):
        """Read SQLite data, split into 200 MB chunks, upload to GCS."""
        date_str = os.path.basename(db_path).split("_")[-1].replace(".db", "")
        gcs_prefix = f"{date_str}"
        chunk_index = 1
        bytes_written = 0
        local_chunk = os.path.join(tempfile.gettempdir(), f"chunk_{chunk_index}.json")
        f = open(local_chunk, "w", encoding="utf-8")
        for line in self.yield_rows(db_path):
            encoded = line + "\n"
            if bytes_written + len(encoded.encode("utf-8")) > self.CHUNK_SIZE_BYTES:
                # close current chunk and upload
                f.close()
                blob_name = f"{gcs_prefix}/{date_str}_chunk_{chunk_index}.json"
                blob = self.bucket.blob(blob_name)
                blob.upload_from_filename(local_chunk)
                print(f"✅ Uploaded {blob_name}")
                os.remove(local_chunk)

                # start new chunk
                chunk_index += 1
                local_chunk = os.path.join(tempfile.gettempdir(), f"chunk_{chunk_index}.json")
                f = open(local_chunk, "w", encoding="utf-8")
                bytes_written = 0

            f.write(encoded)
            bytes_written += len(encoded.encode("utf-8"))

        # Upload last chunk
        f.close()
        if bytes_written > 0:
            blob_name = f"{gcs_prefix}/{date_str}_chunk_{chunk_index}.json"
            blob = self.bucket.blob(blob_name)
            blob.upload_from_filename(local_chunk)
            print(f"✅ Uploaded {blob_name}")
            os.remove(local_chunk)

        print(f"🎯 All chunks uploaded for {date_str}")

gcp_Bucket_Services = Gcp_Bucket_Services(db)
