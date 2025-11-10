
from datetime import datetime, timezone, timedelta
import requests

class GeneralFunctions:
    CHUNK_SIZE_MB = 200
    CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024
    def __init__(self):
        pass
    def slack_service(self, channel_id: str, message: str, day_str: str = None, status: str = "INFO"):
        IST = timezone(timedelta(hours=5, minutes=30))
        # now_ist = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
        now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        day_info = f"📅 Date: {day_str}" if day_str else ""
        emoji = "✅" if status == "SUCCESS" else "❌" if status == "FAILURE" else "ℹ️"
        formatted_message = (
            f"{emoji} **Pipeline Notification**\n"
            f"{day_info}\n"
            f"🕒 Time: {now_ist}\n"
            f"💬 Message: {message}"
        )
        url = "https://client-stage.letsterra.com/emails/slack-message"
        payload = {
            "channelName": channel_id,
            "subject": "Notification from pipline",
            "message": formatted_message,
        }
        try:
            response = requests.post(url, json=payload, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[Slack Error] Failed to send Slack message: {e}")
            return {"error": str(e)}


generalFunctions = GeneralFunctions()