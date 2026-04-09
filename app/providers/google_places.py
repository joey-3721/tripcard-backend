from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.cache_mysql import MySQLCache
from app.providers.base import ProviderPlace

logger = logging.getLogger("tripcard-backend")

GOOGLE_PLACES_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
GOOGLE_PLACES_FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.location",
        "places.addressComponents",
        "places.primaryType",
        "places.types",
    ]
)


async def search_google_places(
    client: httpx.AsyncClient,
    query: str,
    language: str,
    country_filter_code: str | None,
    limit: int,
) -> list[ProviderPlace]:
    return (await fetch_google_places_batch(client, [query], language, country_filter_code, limit)).get(query, [])


async def fetch_google_places_batch(
    client: httpx.AsyncClient,
    queries: list[str],
    language: str,
    country_filter_code: str | None,
    limit: int,
    api_enabled: bool = True,
) -> dict[str, list[ProviderPlace]]:
    deduped_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        normalized = " ".join(str(query).split())
        if not normalized or normalized in seen_queries:
            continue
        seen_queries.add(normalized)
        deduped_queries.append(query)

    if not deduped_queries:
        return {}

    db = MySQLCache()
    cached_rows_by_query = db.get_place_geocode_cache_batch(
        source="google",
        queries=deduped_queries,
        language=language,
        country_filter_code=country_filter_code,
        limit=limit,
    )

    results: dict[str, list[ProviderPlace]] = {}
    missing_queries: list[str] = []
    for query in deduped_queries:
        cached_results = cached_rows_by_query.get(query) or []
        if cached_results:
            logger.info("Google Places 缓存命中 query=%s 条数=%d", query, len(cached_results))
            results[query] = [row_to_provider_place(row) for row in cached_results]
        else:
            missing_queries.append(query)

    if not missing_queries or not api_enabled:
        for query in missing_queries:
            results[query] = []
        return results

    token_row = db.get_ai_token("google")
    if token_row is None:
        logger.warning("Google Places token 未在数据库中配置")
        for query in missing_queries:
            results[query] = []
        return results

    tasks = {
        query: asyncio.create_task(
            _search_google_places_api(
                client=client,
                db=db,
                token_row=token_row,
                query=query,
                language=language,
                country_filter_code=country_filter_code,
                limit=limit,
            )
        )
        for query in missing_queries
    }
    for query in missing_queries:
        try:
            results[query] = await tasks[query]
        except Exception as exc:
            logger.warning("Google Places 接口请求失败 query=%s error=%r", query, exc)
            if isinstance(exc, httpx.HTTPStatusError):
                logger.warning("Google Places 响应体=%s", exc.response.text[:400])
            results[query] = []
    return results


async def _search_google_places_api(
    client: httpx.AsyncClient,
    db: MySQLCache,
    token_row: dict[str, Any],
    query: str,
    language: str,
    country_filter_code: str | None,
    limit: int,
) -> list[ProviderPlace]:
    usage = db.increment_ai_provider_usage("google")
    if usage is None:
        logger.warning("Google Places 用量配置缺失 query=%s", query)
        return []
    if not usage.get("allowed", False):
        logger.warning(
            "Google Places 额度不可用 query=%s 月用量=%s/%s 月份=%s 日用量=%s/%s 日期=%s 原因=%s",
            query,
            usage.get("monthly_call_count", 0),
            usage.get("monthly_limit", -1),
            usage.get("usage_month", ""),
            usage.get("daily_call_count", 0),
            usage.get("daily_limit", -1),
            usage.get("usage_date", ""),
            usage.get("reason", ""),
        )
        return []

    headers = {
        "X-Goog-Api-Key": token_row["token"],
        "X-Goog-FieldMask": GOOGLE_PLACES_FIELD_MASK,
    }
    body: dict[str, Any] = {
        "textQuery": query,
        "languageCode": normalize_google_language(language),
        "pageSize": min(max(limit, 1), 20),
    }
    if country_filter_code:
        body["regionCode"] = str(country_filter_code).lower()

    response = await client.post(
        GOOGLE_PLACES_TEXT_SEARCH_URL,
        headers=headers,
        json=body,
    )
    response.raise_for_status()
    payload: dict[str, Any] = response.json()

    rows = payload.get("places") or []
    cache_rows: list[dict[str, Any]] = []
    items: list[ProviderPlace] = []
    for row in rows:
        item = google_place_to_provider_place(row)
        if item is None:
            continue
        items.append(item)
        cache_rows.append(
            {
                "place_id": item.provider_place_id,
                "name": item.name,
                "address": item.address,
                "subtitle": item.subtitle,
                "latitude": item.latitude,
                "longitude": item.longitude,
                "country": item.country,
                "country_code": item.country_code,
                "locality": item.locality,
                "place_type": item.place_type,
                "category": item.category,
                "full_response": row,
            }
        )

    if cache_rows:
        db.set_place_geocode_cache(
            source="google",
            query=query,
            language=language,
            country_filter_code=country_filter_code,
            rows=cache_rows,
        )

    logger.info(
        "Google Places 接口查询完成并写入缓存 query=%s 结果数=%d 月用量=%s/%s 月份=%s 日用量=%s/%s 日期=%s",
        query,
        len(items),
        usage.get("monthly_call_count", 0),
        usage.get("monthly_limit", -1),
        usage.get("usage_month", ""),
        usage.get("daily_call_count", 0),
        usage.get("daily_limit", -1),
        usage.get("usage_date", ""),
    )
    return items


def row_to_provider_place(row: dict[str, Any]) -> ProviderPlace:
    return ProviderPlace(
        provider="google",
        provider_place_id=row.get("place_id"),
        name=row.get("name", ""),
        subtitle=row.get("subtitle"),
        address=row.get("address"),
        latitude=float(row["latitude"]),
        longitude=float(row["longitude"]),
        country=row.get("country"),
        country_code=(row.get("country_code") or "").upper(),
        locality=row.get("locality"),
        place_type=row.get("place_type"),
        category=row.get("category"),
    )


def google_place_to_provider_place(row: dict[str, Any]) -> ProviderPlace | None:
    location = row.get("location") or {}
    latitude = location.get("latitude")
    longitude = location.get("longitude")
    if latitude is None or longitude is None:
        return None

    display_name = row.get("displayName") or {}
    name = str(display_name.get("text") or "").strip()
    if not name:
        return None

    locality = None
    country = None
    country_code = ""
    for component in row.get("addressComponents") or []:
        component_types = set(component.get("types") or [])
        long_text = str(component.get("longText") or "").strip()
        short_text = str(component.get("shortText") or "").strip()
        if "locality" in component_types and long_text:
            locality = long_text
        elif not locality and "administrative_area_level_1" in component_types and long_text:
            locality = long_text
        if "country" in component_types:
            country = long_text or country
            country_code = short_text or country_code

    primary_type = str(row.get("primaryType") or "").strip() or None
    category = primary_type
    return ProviderPlace(
        provider="google",
        provider_place_id=row.get("id"),
        name=name,
        subtitle=", ".join([part for part in [locality, country] if part]) or None,
        address=row.get("formattedAddress"),
        latitude=float(latitude),
        longitude=float(longitude),
        country=country,
        country_code=country_code.upper(),
        locality=locality,
        place_type=primary_type,
        category=category,
    )


def normalize_google_language(language: str) -> str:
    primary = str(language or "").split(",")[0].strip()
    return primary or "en"
