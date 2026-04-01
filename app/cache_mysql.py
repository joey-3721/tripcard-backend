from __future__ import annotations

import json
import time
from typing import Any

import pymysql

from app.config import settings


class MySQLCache:
    def __init__(self) -> None:
        self.table_name = settings.cache_table_name

    def _connect(self):
        return pymysql.connect(
            host=settings.mysql_host,
            port=settings.mysql_port,
            user=settings.mysql_user,
            password=settings.mysql_password,
            database=settings.mysql_db,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def ensure_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                        cache_key VARCHAR(64) PRIMARY KEY,
                        payload LONGTEXT NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at DATETIME NOT NULL,
                        INDEX idx_expires_at (expires_at)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

    def get(self, cache_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT payload
                    FROM `{self.table_name}`
                    WHERE cache_key = %s
                      AND expires_at > NOW()
                    LIMIT 1
                    """,
                    (cache_key,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return json.loads(row["payload"])

    def set(self, cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
        expires_at = int(time.time()) + ttl_seconds
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO `{self.table_name}` (cache_key, payload, expires_at)
                    VALUES (%s, %s, FROM_UNIXTIME(%s))
                    ON DUPLICATE KEY UPDATE
                        payload = VALUES(payload),
                        expires_at = VALUES(expires_at),
                        created_at = CURRENT_TIMESTAMP
                    """,
                    (cache_key, encoded, expires_at),
                )

    def cleanup(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM `{self.table_name}` WHERE expires_at <= NOW()"
                )
