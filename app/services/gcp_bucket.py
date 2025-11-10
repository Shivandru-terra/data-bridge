import os
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()
db = os.getenv("BUCKET_NAME")

class Gcp_Bucket_Services:
    def __init__(self, bucket_name: str):
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)

gcp_Bucket_Services = Gcp_Bucket_Services(db)
