# from app.services.mixpanel import mixPanelServices
# from app.db.sqlite_store import SqliteStore
# from datetime import date, timedelta


# # today_str = date.today().strftime("%Y-%m-%d")
# yesterday = (date.today() - timedelta(days=2)).strftime("%Y-%m-%d")
# db_path = SqliteStore.db_path_for_date(yesterday)
# sqliteStore = SqliteStore(db_path)
# sqliteStore.init_db()

# batch = []
# batch_size = 5000
# for event in mixPanelServices.fetch_stream(yesterday, yesterday):
#     batch.append(event)
#     if len(batch) >= batch_size:
#         sqliteStore.insert_batch(batch)
#         batch.clear()
# if batch:
#     sqliteStore.insert_batch(batch)

# total = sqliteStore.count_events()
# print(f"✅ Total deduped events stored for {yesterday}: {total}")