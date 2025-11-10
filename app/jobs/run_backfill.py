from datetime import date, timedelta
from app.jobs.run_pipeline import run_pipeline
from app.utils.general_functions import generalFunctions

CHANNEL = "shivandru-self-dm"

def daterange(start_date: date, end_date: date):
    """Yield all dates between start_date and end_date inclusive."""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)

def run_backfill():
    start_date = date(2024, 8, 1)
    end_date = date(2024, 8, 7)
    # end_date = date(2025, 10, 31)

    print(f"🚀 Starting backfill from {start_date} → {end_date}")

    for current_day in daterange(start_date, end_date):
        day_str = current_day.strftime("%Y-%m-%d")
        print(f"\n📅 Processing {day_str} ...")

        try:
            run_pipeline(day_str)
        except Exception as e:
            error_msg = f"❌ Failed pipeline for {day_str}: {str(e)}"
            print(error_msg)
            generalFunctions.slack_service(CHANNEL, error_msg)
            continue  # Skip and move to next day

    generalFunctions.slack_service(
        CHANNEL,
        f"🎯 Backfill complete from {start_date} → {end_date}",
    )
    print("✅ Backfill complete!")


if __name__ == "__main__":
    run_backfill()