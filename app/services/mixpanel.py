import os
import time
import json
import requests
from dotenv import load_dotenv
load_dotenv()

class MixPanelServices:
    URL = "https://data.mixpanel.com/api/2.0/export"
    platform = os.getenv("PLATFORM")
    if platform == "Ripple":
        MIXPANEL_API_KEY = os.getenv("MIXPANEL_API_SECRET_RIPPLE")
    else:
        MIXPANEL_API_KEY = os.getenv("MIXPANEL_API_SECRET_TERRA")
    def __init__(self):
        if not self.MIXPANEL_API_KEY:
            raise ValueError("MIXPANEL_API_KEY is not set")
    
    def fetch_stream(self, from_date: str, to_date: str, max_retries: int = 5):
        params = {
            "from_date": from_date,
            "to_date": to_date,
        }
        backoff = 2
        for attempt in range(1, max_retries + 1):
            try:
                with requests.get(
                    self.URL,
                    params=params,
                    auth=(self.MIXPANEL_API_KEY, ""),
                    timeout=120,
                    stream=True,
                ) as response:
                    response.raise_for_status()
                    for line in response.iter_lines(decode_unicode=True):
                        if line:
                            yield json.loads(line)
                    return
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    print(f"[Retry {attempt}/{max_retries}] Error: {e}")
                    if attempt == max_retries:
                        raise
                    sleep_time = backoff ** attempt
                    print(f"Sleeping {sleep_time}s before retry...")
                    time.sleep(sleep_time)
    
    def fetch_events(self, from_date: str, to_date: str):
        """
        Fetch events efficiently. Streams line by line and optionally writes to disk.
        """
        return self.fetch_stream(from_date, to_date)

mixPanelServices = MixPanelServices()