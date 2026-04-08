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

    def get_ai_cache(self, cache_key: str) -> dict | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM `ai_parse_cache` "
                    "WHERE cache_key = %s AND expires_at > NOW() LIMIT 1",
                    (cache_key,),
                )
                row = cur.fetchone()
                return json.loads(row["payload"]) if row else None

    def set_ai_cache(self, cache_key: str, payload: dict, ttl_seconds: int = 86400 * 7) -> None:
        expires_at = int(time.time()) + ttl_seconds
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO `ai_parse_cache` (cache_key, payload, expires_at) "
                    "VALUES (%s, %s, FROM_UNIXTIME(%s)) "
                    "ON DUPLICATE KEY UPDATE payload=VALUES(payload), expires_at=VALUES(expires_at), created_at=CURRENT_TIMESTAMP",
                    (cache_key, encoded, expires_at),
                )

    def ensure_ai_parse_cache_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS `ai_parse_cache` (
                        cache_key VARCHAR(64) PRIMARY KEY,
                        payload   LONGTEXT NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at DATETIME NOT NULL,
                        INDEX idx_expires_at (expires_at)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

    def ensure_gaode_cache_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS `gaode_geocode_cache` (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        query_key VARCHAR(255) NOT NULL,
                        query_text VARCHAR(500) NOT NULL,
                        poi_id VARCHAR(64),
                        poi_name VARCHAR(255) NOT NULL,
                        poi_type VARCHAR(100),
                        poi_typecode VARCHAR(100),
                        latitude DECIMAL(10, 7) NOT NULL,
                        longitude DECIMAL(11, 7) NOT NULL,
                        province VARCHAR(100),
                        city VARCHAR(100),
                        district VARCHAR(100),
                        address TEXT,
                        full_response JSON,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY idx_query_poi (query_key, poi_id),
                        INDEX idx_query_key (query_key),
                        INDEX idx_poi_name (poi_name)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

    def get_gaode_cache(self, query: str, limit: int = 20) -> list[dict] | None:
        """Get cached Gaode results for a query."""
        import hashlib
        query_key = hashlib.md5(query.encode('utf-8')).hexdigest()

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT poi_id, poi_name, poi_type, poi_typecode,
                           latitude, longitude, province, city, district, address
                    FROM `gaode_geocode_cache`
                    WHERE query_key = %s
                    ORDER BY id
                    LIMIT %s
                    """,
                    (query_key, limit),
                )
                rows = cur.fetchall()
                return rows if rows else None

    def set_gaode_cache(self, query: str, pois: list[dict]) -> None:
        """Cache Gaode API results."""
        import hashlib
        query_key = hashlib.md5(query.encode('utf-8')).hexdigest()

        with self._connect() as conn:
            with conn.cursor() as cur:
                for poi in pois:
                    location = poi.get("location", "")
                    if not location or "," not in location:
                        continue

                    try:
                        lon_str, lat_str = location.split(",", 1)
                        longitude = float(lon_str)
                        latitude = float(lat_str)
                    except (ValueError, AttributeError):
                        continue

                    cur.execute(
                        """
                        INSERT INTO `gaode_geocode_cache`
                        (query_key, query_text, poi_id, poi_name, poi_type, poi_typecode,
                         latitude, longitude, province, city, district, address, full_response)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            poi_name = VALUES(poi_name),
                            poi_type = VALUES(poi_type),
                            poi_typecode = VALUES(poi_typecode),
                            latitude = VALUES(latitude),
                            longitude = VALUES(longitude),
                            province = VALUES(province),
                            city = VALUES(city),
                            district = VALUES(district),
                            address = VALUES(address),
                            full_response = VALUES(full_response),
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            query_key,
                            query,
                            poi.get("id"),
                            poi.get("name", ""),
                            poi.get("type"),
                            poi.get("typecode"),
                            latitude,
                            longitude,
                            poi.get("pname"),
                            poi.get("cityname"),
                            poi.get("adname"),
                            poi.get("address"),
                            json.dumps(poi, ensure_ascii=False),
                        ),
                    )
