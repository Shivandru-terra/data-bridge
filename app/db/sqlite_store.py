import sqlite3
import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

DEFAULT_DB_DIR = "/tmp" if os.name != "nt" else os.path.join(os.getcwd(), "tmp_sqlite")

class SqliteStore:
    TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS mixpanel_events (
    insert_id TEXT PRIMARY KEY,
    event_name TEXT,
    distinct_id TEXT,
    event_time INTEGER,
    raw_json TEXT NOT NULL
);
    """
    INDEX_SCHEMA = """
    CREATE INDEX IF NOT EXISTS idx_event_time ON mixpanel_events(event_time);
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
    
    @staticmethod
    def ensure_db_dir(db_dir: str = DEFAULT_DB_DIR):
        os.makedirs(db_dir, exist_ok=True)
        return db_dir
    
    @staticmethod
    def db_path_for_date(date_str: str, db_dir: str = DEFAULT_DB_DIR) -> str:
        """
        date_str expected like '2024-05-20' (YYYY-MM-DD)
        Returns a path like /tmp/mixpanel_dedupe_2024-05-20.db
        """
        SqliteStore.ensure_db_dir(db_dir)
        filename = f"mixpanel_dedupe_{date_str}.db"
        return os.path.join(db_dir, filename)
    
    def init_db(self, pragmas: Optional[dict] = None):
        if pragmas is None:
            pragmas = {
                "journal_mode": "WAL",
                "synchronous": "NORMAL",
                "temp_store": "MEMORY",
                "cache_size": -20000,
            }
        self.conn = sqlite3.connect(self.db_path, timeout=60)
        cur = self.conn.cursor()

        for k, v in pragmas.items():
            cur.execute(f"PRAGMA {k}={v};")
        
        cur.execute(self.TABLE_SCHEMA)
        cur.execute(self.INDEX_SCHEMA)
        self.conn.commit()

        print(f"✅ SQLite initialized at {self.db_path}")
        return self.conn
    
    def insert_batch(self, events: List[Dict[str, Any]]):
        """
        Insert a batch of Mixpanel events. Automatically skips duplicates.
        Each event must contain 'insert_id' in its properties.
        """
        if not events:
            return
        
        cur = self.conn.cursor()
        rows = []
        for e in events:
            props = e.get('properties', {})
            insert_id = props.get('insert_id') or props.get('$insert_id')
            if not insert_id:
                continue  # skip events without insert_id
            event_name = e.get('event')
            distinct_id = e.get('properties', {}).get('distinct_id')
            event_time = e.get('properties', {}).get('time')
            raw_json = json.dumps(e)
            rows.append((insert_id, event_name, distinct_id, event_time, raw_json))

        cur.executemany("""
            INSERT OR IGNORE INTO mixpanel_events 
            (insert_id, event_name, distinct_id, event_time, raw_json)
            VALUES (?, ?, ?, ?, ?);
        """, rows)

        self.conn.commit()
    def close(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
    
    def count_events(self) -> int:
        """Return total rows in DB (after dedup)."""
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM mixpanel_events;")
        return cur.fetchone()[0]