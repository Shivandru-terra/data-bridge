# import os
# from dotenv import load_dotenv
# from app.services.gcp_bucket import gcp_Bucket_Services
# load_dotenv()

# def main():
#     db_path = "tmp_sqlite/mixpanel_dedupe_2025-11-08.db"
#     if not os.path.exists(db_path):
#         print(f"❌ File not found: {db_path}")
#         return
    
#     print(f"🚀 Uploading {db_path} to GCS ...")
#     gcp_Bucket_Services.upload_day(db_path)
#     print("✅ Upload complete!")

# if __name__ == "__main__":
#     main()
