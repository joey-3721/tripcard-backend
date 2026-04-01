from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class SQLiteCache:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS search_cache (
                    cache_key TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                )
                """
            )
            conn.commit()

    def get(self, cache_key: str, ttl_seconds: int) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT payload, created_at FROM search_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            if row is None:
                return None
            if int(time.time()) - int(row["created_at"]) > ttl_seconds:
                conn.execute("DELETE FROM search_cache WHERE cache_key = ?", (cache_key,))
                conn.commit()
                return None
            return json.loads(row["payload"])

    def set(self, cache_key: str, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False)
        now = int(time.time())
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO search_cache (cache_key, payload, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload = excluded.payload,
                    created_at = excluded.created_at
                """,
                (cache_key, encoded, now),
            )
            conn.commit()
