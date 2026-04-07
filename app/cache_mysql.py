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

    def ensure_ai_tokens_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{settings.ai_tokens_table_name}` (
                        id          INT AUTO_INCREMENT PRIMARY KEY,
                        provider    VARCHAR(32)  NOT NULL,
                        token       VARCHAR(512) NOT NULL,
                        model       VARCHAR(64)  NOT NULL DEFAULT 'deepseek-chat',
                        base_url    VARCHAR(256) NOT NULL DEFAULT 'https://api.deepseek.com',
                        enabled     TINYINT(1)   NOT NULL DEFAULT 1,
                        created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_provider_enabled (provider, enabled)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

    def get_ai_token(self, provider: str = "deepseek") -> dict | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT token, model, base_url FROM `{settings.ai_tokens_table_name}` "
                    "WHERE provider = %s AND enabled = 1 LIMIT 1",
                    (provider,),
                )
                return cur.fetchone()
