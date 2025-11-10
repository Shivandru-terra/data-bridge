import os
import time
import json
import requests
from dotenv import load_dotenv
from app.utils.general_functions import generalFunctions
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
    
    def fetch_stream(self, from_date: str, to_date: str, max_retries: int = 5, slack_channel: str = None):
        params = {
            "from_date": from_date,
            "to_date": to_date,
        }
        backoff = 5
        for attempt in range(1, max_retries + 1):
            try:
                print(f"📡 Fetching Mixpanel events for {from_date} ... Attempt {attempt}/{max_retries}")
                with requests.get(
                    self.URL,
                    params=params,
                    auth=(self.MIXPANEL_API_KEY, ""),
                    timeout=120,
                    stream=True,
                ) as response:
                    if response.status_code == 429:
                        wait_time = backoff * attempt + 5
                        print(f"⚠️ Rate limited by Mixpanel. Sleeping {wait_time}s ...")
                        time.sleep(wait_time)
                        continue
                    response.raise_for_status()
                    for line in response.iter_lines(decode_unicode=True):
                        if line:
                            yield json.loads(line)
                    print(f"✅ Successfully fetched events for {from_date}")
                    return
            except requests.exceptions.RequestException as e:
                print(f"❌ Error fetching Mixpanel data for {from_date}: {e}")
                if attempt < max_retries:
                    sleep_time = backoff * attempt
                    print(f"Retrying in {sleep_time}s ...")
                    time.sleep(sleep_time)
                else:
                    if slack_channel:
                        generalFunctions.slack_service(
                            slack_channel,
                            f"Mixpanel fetch failed for {from_date}: {e}",
                        )
                    raise
    
    def fetch_events(self, from_date: str, to_date: str, slack_channel: str = None):
        """
        Fetch events efficiently. Streams line by line and optionally writes to disk.
        """
        events = []
        for event in self.fetch_stream(from_date, to_date, slack_channel=slack_channel):
            events.append(event)
        return events

mixPanelServices = MixPanelServices()