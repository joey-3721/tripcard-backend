from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import pymysql

from app.config import settings


class MySQLCache:
    def __init__(self) -> None:
        self.table_name = settings.cache_table_name
        self.place_fuzzy_cache_table_name = "place_fuzzy_search_cache"

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
            init_command="SET time_zone = '+08:00'",
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

    def ensure_place_fuzzy_search_cache_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{self.place_fuzzy_cache_table_name}` (
                        cache_key VARCHAR(64) PRIMARY KEY,
                        payload LONGTEXT NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at DATETIME NOT NULL,
                        INDEX idx_expires_at (expires_at)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )

    def get_place_fuzzy_search_cache(self, cache_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT payload
                    FROM `{self.place_fuzzy_cache_table_name}`
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

    def set_place_fuzzy_search_cache(self, cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
        expires_at = int(time.time()) + ttl_seconds
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO `{self.place_fuzzy_cache_table_name}` (cache_key, payload, expires_at)
                    VALUES (%s, %s, FROM_UNIXTIME(%s))
                    ON DUPLICATE KEY UPDATE
                        payload = VALUES(payload),
                        expires_at = VALUES(expires_at),
                        created_at = CURRENT_TIMESTAMP
                    """,
                    (cache_key, encoded, expires_at),
                )

    def cleanup_place_fuzzy_search_cache(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM `{self.place_fuzzy_cache_table_name}` WHERE expires_at <= NOW()"
                )

    def ensure_ai_tokens_table(self) -> None:
        current_month = self._current_usage_month()
        current_date = self._current_usage_date()
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
                        monthly_call_count INT NOT NULL DEFAULT 0,
                        monthly_limit      INT NOT NULL DEFAULT -1,
                        usage_month        VARCHAR(7) NOT NULL DEFAULT '',
                        daily_call_count   INT NOT NULL DEFAULT 0,
                        daily_limit        INT NOT NULL DEFAULT -1,
                        usage_date         VARCHAR(10) NOT NULL DEFAULT '',
                        enabled     TINYINT(1)   NOT NULL DEFAULT 1,
                        created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_provider_enabled (provider, enabled)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name=settings.ai_tokens_table_name,
                    column_name="monthly_call_count",
                    definition="INT NOT NULL DEFAULT 0",
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name=settings.ai_tokens_table_name,
                    column_name="monthly_limit",
                    definition="INT NOT NULL DEFAULT -1",
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name=settings.ai_tokens_table_name,
                    column_name="usage_month",
                    definition="VARCHAR(7) NOT NULL DEFAULT ''",
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name=settings.ai_tokens_table_name,
                    column_name="daily_call_count",
                    definition="INT NOT NULL DEFAULT 0",
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name=settings.ai_tokens_table_name,
                    column_name="daily_limit",
                    definition="INT NOT NULL DEFAULT -1",
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name=settings.ai_tokens_table_name,
                    column_name="usage_date",
                    definition="VARCHAR(10) NOT NULL DEFAULT ''",
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET usage_month = %s
                    WHERE usage_month = ''
                    """,
                    (current_month,),
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET usage_date = %s
                    WHERE usage_date = ''
                    """,
                    (current_date,),
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET monthly_limit = 3000
                    WHERE provider = 'google' AND monthly_limit = 0
                    """,
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET daily_limit = 3000
                    WHERE provider = 'geoapify' AND daily_limit = 0
                    """,
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET monthly_limit = -1
                    WHERE monthly_limit = 0
                    """,
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET daily_limit = -1
                    WHERE daily_limit = 0
                    """,
                )

    def get_ai_token(self, provider: str = "deepseek") -> dict | None:
        self.reset_ai_provider_usage_if_needed(provider)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT token, model, base_url, monthly_call_count, monthly_limit, usage_month, daily_call_count, daily_limit, usage_date FROM `{settings.ai_tokens_table_name}` "
                    "WHERE provider = %s AND enabled = 1 LIMIT 1",
                    (provider,),
                )
                return cur.fetchone()

    def reset_ai_provider_usage_if_needed(self, provider: str) -> None:
        current_month = self._current_usage_month()
        current_date = self._current_usage_date()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET monthly_call_count = 0,
                        usage_month = %s
                    WHERE provider = %s
                      AND enabled = 1
                      AND usage_month <> %s
                    """,
                    (current_month, provider, current_month),
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET daily_call_count = 0,
                        usage_date = %s
                    WHERE provider = %s
                      AND enabled = 1
                      AND usage_date <> %s
                    """,
                    (current_date, provider, current_date),
                )

    def increment_ai_provider_usage(self, provider: str, amount: int = 1) -> dict | None:
        current_month = self._current_usage_month()
        current_date = self._current_usage_date()
        amount = max(1, int(amount))
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET monthly_call_count = CASE
                            WHEN usage_month = %s THEN monthly_call_count
                            ELSE 0
                        END,
                        usage_month = %s
                    WHERE provider = %s
                      AND enabled = 1
                    """,
                    (current_month, current_month, provider),
                )
                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET daily_call_count = CASE
                            WHEN usage_date = %s THEN daily_call_count
                            ELSE 0
                        END,
                        usage_date = %s
                    WHERE provider = %s
                      AND enabled = 1
                    """,
                    (current_date, current_date, provider),
                )
                cur.execute(
                    f"""
                    SELECT id, monthly_call_count, monthly_limit, usage_month,
                           daily_call_count, daily_limit, usage_date
                    FROM `{settings.ai_tokens_table_name}`
                    WHERE provider = %s
                      AND enabled = 1
                    LIMIT 1
                    """,
                    (provider,),
                )
                row = cur.fetchone()
                if row is None:
                    return None

                monthly_limit = int(row.get("monthly_limit") if row.get("monthly_limit") is not None else -1)
                monthly_call_count = int(row.get("monthly_call_count") or 0)
                daily_limit = int(row.get("daily_limit") if row.get("daily_limit") is not None else -1)
                daily_call_count = int(row.get("daily_call_count") or 0)
                if monthly_limit == 0:
                    return {
                        "provider": provider,
                        "monthly_call_count": monthly_call_count,
                        "monthly_limit": monthly_limit,
                        "usage_month": row.get("usage_month") or current_month,
                        "daily_call_count": daily_call_count,
                        "daily_limit": daily_limit,
                        "usage_date": row.get("usage_date") or current_date,
                        "allowed": False,
                        "reason": "monthly_disabled",
                    }
                if daily_limit == 0:
                    return {
                        "provider": provider,
                        "monthly_call_count": monthly_call_count,
                        "monthly_limit": monthly_limit,
                        "usage_month": row.get("usage_month") or current_month,
                        "daily_call_count": daily_call_count,
                        "daily_limit": daily_limit,
                        "usage_date": row.get("usage_date") or current_date,
                        "allowed": False,
                        "reason": "daily_disabled",
                    }
                if monthly_limit > 0 and monthly_call_count + amount > monthly_limit:
                    return {
                        "provider": provider,
                        "monthly_call_count": monthly_call_count,
                        "monthly_limit": monthly_limit,
                        "usage_month": row.get("usage_month") or current_month,
                        "daily_call_count": daily_call_count,
                        "daily_limit": daily_limit,
                        "usage_date": row.get("usage_date") or current_date,
                        "allowed": False,
                        "reason": "monthly_limit_reached",
                    }
                if daily_limit > 0 and daily_call_count + amount > daily_limit:
                    return {
                        "provider": provider,
                        "monthly_call_count": monthly_call_count,
                        "monthly_limit": monthly_limit,
                        "usage_month": row.get("usage_month") or current_month,
                        "daily_call_count": daily_call_count,
                        "daily_limit": daily_limit,
                        "usage_date": row.get("usage_date") or current_date,
                        "allowed": False,
                        "reason": "daily_limit_reached",
                    }

                cur.execute(
                    f"""
                    UPDATE `{settings.ai_tokens_table_name}`
                    SET monthly_call_count = monthly_call_count + %s,
                        daily_call_count = daily_call_count + %s
                    WHERE id = %s
                    """,
                    (amount, amount, row["id"]),
                )
                return {
                    "provider": provider,
                    "monthly_call_count": monthly_call_count + amount,
                    "monthly_limit": monthly_limit,
                    "usage_month": row.get("usage_month") or current_month,
                    "daily_call_count": daily_call_count + amount,
                    "daily_limit": daily_limit,
                    "usage_date": row.get("usage_date") or current_date,
                    "allowed": True,
                }

    def get_ai_cache(self, cache_key: str) -> dict | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT payload FROM `ai_parse_cache` "
                    "WHERE cache_key = %s LIMIT 1",
                    (cache_key,),
                )
                row = cur.fetchone()
                return json.loads(row["payload"]) if row else None

    def set_ai_cache(self, cache_key: str, payload: dict, ttl_seconds: int = 86400 * 7) -> None:
        encoded = json.dumps(payload, ensure_ascii=False)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO `ai_parse_cache` (cache_key, payload) "
                    "VALUES (%s, %s) "
                    "ON DUPLICATE KEY UPDATE payload=VALUES(payload), created_at=CURRENT_TIMESTAMP",
                    (cache_key, encoded),
                )

    def ensure_ai_parse_cache_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS `ai_parse_cache` (
                        cache_key VARCHAR(64) PRIMARY KEY,
                        payload   LONGTEXT NOT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name="ai_parse_cache",
                    column_name="created_at",
                    definition="DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
                )
                self._drop_index_if_exists(
                    cur=cur,
                    table_name="ai_parse_cache",
                    index_name="idx_expires_at",
                )
                self._drop_column_if_exists(
                    cur=cur,
                    table_name="ai_parse_cache",
                    column_name="expires_at",
                )

    def ensure_ai_parse_jobs_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS `ai_parse_jobs` (
                        task_id VARCHAR(64) PRIMARY KEY,
                        cache_key VARCHAR(64) NOT NULL,
                        provider VARCHAR(32) NOT NULL,
                        language VARCHAR(32) NOT NULL DEFAULT 'zh-CN',
                        status VARCHAR(16) NOT NULL DEFAULT 'queued',
                        progress INT NOT NULL DEFAULT 0,
                        message VARCHAR(255) NOT NULL DEFAULT '',
                        error_message TEXT NULL,
                        request_payload LONGTEXT NULL,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_cache_key_status (cache_key, status),
                        INDEX idx_created_at (created_at)
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

    def ensure_place_geocode_cache_table(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS `place_geocode_cache` (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        source VARCHAR(32) NOT NULL,
                        query_key VARCHAR(64) NOT NULL,
                        query_text VARCHAR(500) NOT NULL,
                        language VARCHAR(32) NOT NULL DEFAULT 'en',
                        country_filter_code VARCHAR(8) NULL,
                        cache_item_key VARCHAR(160) NOT NULL,
                        place_id VARCHAR(128),
                        name VARCHAR(255) NOT NULL,
                        address TEXT,
                        subtitle VARCHAR(255),
                        latitude DECIMAL(10, 7) NOT NULL,
                        longitude DECIMAL(11, 7) NOT NULL,
                        country VARCHAR(120),
                        country_code VARCHAR(8),
                        locality VARCHAR(120),
                        place_type VARCHAR(120),
                        category VARCHAR(120),
                        hit_count INT NOT NULL DEFAULT 0,
                        full_response JSON,
                        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY idx_source_query_item (source, query_key, cache_item_key),
                        INDEX idx_source_query_key (source, query_key),
                        INDEX idx_name (name)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE `place_geocode_cache`
                    MODIFY COLUMN `cache_item_key` VARCHAR(255) NOT NULL,
                    MODIFY COLUMN `place_id` VARCHAR(512) NULL
                    """
                )
                self._ensure_column_exists(
                    cur=cur,
                    table_name="place_geocode_cache",
                    column_name="hit_count",
                    definition="INT NOT NULL DEFAULT 0",
                )

    def get_gaode_cache(self, query: str, limit: int = 20) -> list[dict] | None:
        """Get cached Gaode results for a query."""
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

    def get_geoapify_cache(
        self,
        query: str,
        language: str,
        country_filter_code: str | None,
        limit: int = 20,
    ) -> list[dict] | None:
        query_key = self._geocode_query_key("geoapify", query, language, country_filter_code)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT place_id, name, address, subtitle,
                           latitude, longitude, country, country_code, locality, place_type, category
                    FROM `place_geocode_cache`
                    WHERE source = %s AND query_key = %s
                    ORDER BY id
                    LIMIT %s
                    """,
                    ("geoapify", query_key, limit),
                )
                rows = cur.fetchall()
                return rows if rows else None

    def set_geoapify_cache(
        self,
        query: str,
        language: str,
        country_filter_code: str | None,
        results: list[dict],
    ) -> None:
        query_key = self._geocode_query_key("geoapify", query, language, country_filter_code)
        normalized_filter = (country_filter_code or "").upper() or None

        with self._connect() as conn:
            with conn.cursor() as cur:
                for row in results:
                    lat = row.get("lat")
                    lon = row.get("lon")
                    if lat is None or lon is None:
                        continue

                    name = str(row.get("name") or row.get("formatted") or "").strip()
                    if not name:
                        continue

                    locality = (
                        row.get("city")
                        or row.get("town")
                        or row.get("village")
                        or row.get("suburb")
                        or row.get("state")
                    )
                    country = row.get("country")
                    subtitle = ", ".join([part for part in [locality, country] if part]) or None

                    cur.execute(
                        """
                        INSERT INTO `place_geocode_cache`
                        (source, query_key, query_text, language, country_filter_code, cache_item_key, place_id, name, address, subtitle,
                         latitude, longitude, country, country_code, locality, place_type, category, full_response)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            name = VALUES(name),
                            address = VALUES(address),
                            subtitle = VALUES(subtitle),
                            latitude = VALUES(latitude),
                            longitude = VALUES(longitude),
                            country = VALUES(country),
                            country_code = VALUES(country_code),
                            locality = VALUES(locality),
                            place_type = VALUES(place_type),
                            category = VALUES(category),
                            full_response = VALUES(full_response),
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            "geoapify",
                            query_key,
                            query,
                            (language or "en")[:32],
                            normalized_filter,
                            self._cache_item_key(
                                place_id=row.get("place_id"),
                                name=name,
                                latitude=float(lat),
                                longitude=float(lon),
                            ),
                            row.get("place_id"),
                            name,
                            row.get("formatted"),
                            subtitle,
                            float(lat),
                            float(lon),
                            country,
                            (row.get("country_code") or "").upper(),
                            locality,
                            row.get("result_type"),
                            row.get("result_type"),
                            json.dumps(row, ensure_ascii=False),
                        ),
                    )

    def get_place_geocode_cache(self, source: str, query: str, language: str, country_filter_code: str | None, limit: int = 20) -> list[dict] | None:
        query_key = self._geocode_query_key(source, query, language, country_filter_code)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE `place_geocode_cache`
                    SET hit_count = hit_count + 1
                    WHERE source = %s AND query_key = %s
                    """,
                    (source, query_key),
                )
                cur.execute(
                    """
                    SELECT place_id, name, address, subtitle,
                           latitude, longitude, country, country_code, locality, place_type, category
                    FROM `place_geocode_cache`
                    WHERE source = %s AND query_key = %s
                    ORDER BY id
                    LIMIT %s
                    """,
                    (source, query_key, limit),
                )
                rows = cur.fetchall()
                return rows if rows else None

    def get_place_geocode_cache_batch(
        self,
        source: str,
        queries: list[str],
        language: str,
        country_filter_code: str | None,
        limit: int = 20,
    ) -> dict[str, list[dict]]:
        unique_queries: list[str] = []
        query_keys: list[str] = []
        key_to_query: dict[str, str] = {}
        seen_keys: set[str] = set()

        for query in queries:
            query_key = self._geocode_query_key(source, query, language, country_filter_code)
            if query_key in seen_keys:
                continue
            seen_keys.add(query_key)
            unique_queries.append(query)
            query_keys.append(query_key)
            key_to_query[query_key] = query

        if not query_keys:
            return {}

        placeholders = ",".join(["%s"] * len(query_keys))
        grouped: dict[str, list[dict]] = {query: [] for query in unique_queries}

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE `place_geocode_cache`
                    SET hit_count = hit_count + 1
                    WHERE source = %s AND query_key IN ({placeholders})
                    """,
                    (source, *query_keys),
                )
                cur.execute(
                    f"""
                    SELECT query_key, place_id, name, address, subtitle,
                           latitude, longitude, country, country_code, locality, place_type, category
                    FROM `place_geocode_cache`
                    WHERE source = %s AND query_key IN ({placeholders})
                    ORDER BY id
                    """,
                    (source, *query_keys),
                )
                rows = cur.fetchall()

        for row in rows:
            query = key_to_query.get(row["query_key"])
            if not query:
                continue
            items = grouped[query]
            if len(items) >= limit:
                continue
            items.append(
                {
                    "place_id": row.get("place_id"),
                    "name": row.get("name"),
                    "address": row.get("address"),
                    "subtitle": row.get("subtitle"),
                    "latitude": row.get("latitude"),
                    "longitude": row.get("longitude"),
                    "country": row.get("country"),
                    "country_code": row.get("country_code"),
                    "locality": row.get("locality"),
                    "place_type": row.get("place_type"),
                    "category": row.get("category"),
                }
            )

        return {query: items for query, items in grouped.items() if items}

    def set_place_geocode_cache(
        self,
        source: str,
        query: str,
        language: str,
        country_filter_code: str | None,
        rows: list[dict],
    ) -> None:
        query_key = self._geocode_query_key(source, query, language, country_filter_code)
        normalized_filter = (country_filter_code or "").upper() or None

        with self._connect() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    latitude = row.get("latitude")
                    longitude = row.get("longitude")
                    if latitude is None or longitude is None:
                        continue

                    name = str(row.get("name") or "").strip()
                    if not name:
                        continue

                    cur.execute(
                        """
                        INSERT INTO `place_geocode_cache`
                        (source, query_key, query_text, language, country_filter_code, cache_item_key, place_id, name, address, subtitle,
                         latitude, longitude, country, country_code, locality, place_type, category, full_response)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            name = VALUES(name),
                            address = VALUES(address),
                            subtitle = VALUES(subtitle),
                            latitude = VALUES(latitude),
                            longitude = VALUES(longitude),
                            country = VALUES(country),
                            country_code = VALUES(country_code),
                            locality = VALUES(locality),
                            place_type = VALUES(place_type),
                            category = VALUES(category),
                            full_response = VALUES(full_response),
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            source,
                            query_key,
                            query,
                            (language or "en")[:32],
                            normalized_filter,
                            self._cache_item_key(
                                place_id=row.get("place_id"),
                                name=name,
                                latitude=float(latitude),
                                longitude=float(longitude),
                            ),
                            row.get("place_id"),
                            name,
                            row.get("address"),
                            row.get("subtitle"),
                            float(latitude),
                            float(longitude),
                            row.get("country"),
                            (row.get("country_code") or "").upper(),
                            row.get("locality"),
                            row.get("place_type"),
                            row.get("category"),
                            json.dumps(row.get("full_response"), ensure_ascii=False) if row.get("full_response") is not None else None,
                        ),
                    )

    def get_ai_job(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT task_id, cache_key, provider, language, status, progress, message, error_message, request_payload, created_at, updated_at
                    FROM `ai_parse_jobs`
                    WHERE task_id = %s
                    LIMIT 1
                    """,
                    (task_id,),
                )
                return cur.fetchone()

    def get_active_ai_job_by_cache_key(self, cache_key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT task_id, cache_key, provider, language, status, progress, message, error_message, request_payload, created_at, updated_at
                    FROM `ai_parse_jobs`
                    WHERE cache_key = %s
                      AND status IN ('queued', 'processing')
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (cache_key,),
                )
                return cur.fetchone()

    def set_ai_job(
        self,
        task_id: str,
        cache_key: str,
        provider: str,
        language: str,
        request_payload: dict[str, Any],
        status: str = "queued",
        progress: int = 0,
        message: str = "",
    ) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO `ai_parse_jobs`
                    (task_id, cache_key, provider, language, status, progress, message, request_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        cache_key = VALUES(cache_key),
                        provider = VALUES(provider),
                        language = VALUES(language),
                        status = VALUES(status),
                        progress = VALUES(progress),
                        message = VALUES(message),
                        request_payload = VALUES(request_payload),
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (
                        task_id,
                        cache_key,
                        provider,
                        (language or "zh-CN")[:32],
                        status,
                        max(0, min(100, int(progress))),
                        message[:255],
                        json.dumps(request_payload, ensure_ascii=False),
                    ),
                )

    def update_ai_job_progress(
        self,
        task_id: str,
        *,
        status: str | None = None,
        progress: int | None = None,
        message: str | None = None,
        error_message: str | None = None,
    ) -> None:
        fields: list[str] = []
        params: list[Any] = []
        if status is not None:
            fields.append("status = %s")
            params.append(status)
        if progress is not None:
            fields.append("progress = %s")
            params.append(max(0, min(100, int(progress))))
        if message is not None:
            fields.append("message = %s")
            params.append(message[:255])
        if error_message is not None:
            fields.append("error_message = %s")
            params.append(error_message)
        if not fields:
            return
        fields.append("updated_at = CURRENT_TIMESTAMP")
        params.append(task_id)
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE `ai_parse_jobs`
                    SET {", ".join(fields)}
                    WHERE task_id = %s
                    """,
                    tuple(params),
                )

    def _geocode_query_key(self, source: str, query: str, language: str, country_filter_code: str | None) -> str:
        payload = "|".join(
            [
                source.strip().lower(),
                query.strip(),
                (language or "en").strip(),
                (country_filter_code or "").strip().upper(),
            ]
        )
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

    def _cache_item_key(self, place_id: str | None, name: str, latitude: float, longitude: float) -> str:
        if place_id:
            normalized_place_id = str(place_id)
            if len(normalized_place_id) <= 200:
                return normalized_place_id
            return hashlib.md5(normalized_place_id.encode("utf-8")).hexdigest()
        fallback = f"{name.strip().lower()}|{latitude:.6f}|{longitude:.6f}"
        return hashlib.md5(fallback.encode("utf-8")).hexdigest()

    def _current_usage_month(self) -> str:
        return time.strftime("%Y-%m")

    def _current_usage_date(self) -> str:
        return time.strftime("%Y-%m-%d")

    def _ensure_column_exists(self, cur, table_name: str, column_name: str, definition: str) -> None:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (settings.mysql_db, table_name, column_name),
        )
        if cur.fetchone():
            return

        cur.execute(
            f"""
            ALTER TABLE `{table_name}`
            ADD COLUMN `{column_name}` {definition}
            """
        )

    def _drop_column_if_exists(self, cur, table_name: str, column_name: str) -> None:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (settings.mysql_db, table_name, column_name),
        )
        if not cur.fetchone():
            return

        cur.execute(
            f"""
            ALTER TABLE `{table_name}`
            DROP COLUMN `{column_name}`
            """
        )

    def _drop_index_if_exists(self, cur, table_name: str, index_name: str) -> None:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND INDEX_NAME = %s
            LIMIT 1
            """,
            (settings.mysql_db, table_name, index_name),
        )
        if not cur.fetchone():
            return

        cur.execute(
            f"""
            ALTER TABLE `{table_name}`
            DROP INDEX `{index_name}`
            """
        )
