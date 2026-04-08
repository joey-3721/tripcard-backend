from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.cache_mysql import MySQLCache
from app.providers.base import ProviderPlace

logger = logging.getLogger("tripcard-backend")

GEOAPIFY_BASE_URL = "https://api.geoapify.com/v1/geocode/search"


async def search_geoapify(
    client: httpx.AsyncClient,
    query: str,
    language: str,
    country_filter_code: str | None,
    limit: int,
) -> list[ProviderPlace]:
    return (await fetch_geoapify_batch(client, [query], language, country_filter_code, limit)).get(query, [])


async def fetch_geoapify_batch(
    client: httpx.AsyncClient,
    queries: list[str],
    language: str,
    country_filter_code: str | None,
    limit: int,
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
        source="geoapify",
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
            logger.info("geoapify cache hit query=%s results=%d", query, len(cached_results))
            results[query] = [row_to_provider_place(row) for row in cached_results]
        else:
            missing_queries.append(query)

    if not missing_queries:
        return results

    token_row = db.get_ai_token("geoapify")
    if token_row is None:
        logger.warning("geoapify token not found in database")
        for query in missing_queries:
            results[query] = []
        return results

    tasks = {
        query: asyncio.create_task(
            _search_geoapify_api(
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
            logger.warning("geoapify api failed query=%s error=%r", query, exc)
            if isinstance(exc, httpx.HTTPStatusError):
                logger.warning("geoapify response body=%s", exc.response.text[:300])
            results[query] = []
    return results


async def _search_geoapify_api(
    client: httpx.AsyncClient,
    db: MySQLCache,
    token_row: dict[str, Any],
    query: str,
    language: str,
    country_filter_code: str | None,
    limit: int,
) -> list[ProviderPlace]:
    params = {
        "text": query,
        "apiKey": token_row["token"],
        "lang": normalize_geoapify_language(language),
        "format": "json",
        "limit": min(max(limit, 1), 20),
        "bias": "countrycode:none",
    }
    if country_filter_code:
        params["filter"] = f"countrycode:{country_filter_code.lower()}"

    response = await client.get(GEOAPIFY_BASE_URL, params=params)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()

    rows = payload.get("results") or []
    if rows:
        cache_rows: list[dict] = []
        for row in rows:
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

            cache_rows.append(
                {
                    "place_id": row.get("place_id"),
                    "name": name,
                    "address": row.get("formatted"),
                    "subtitle": ", ".join([part for part in [locality, country] if part]) or None,
                    "latitude": float(lat),
                    "longitude": float(lon),
                    "country": country,
                    "country_code": (row.get("country_code") or "").upper(),
                    "locality": locality,
                    "place_type": row.get("result_type"),
                    "category": row.get("result_type"),
                    "full_response": row,
                }
            )

        if cache_rows:
            db.set_place_geocode_cache(
                source="geoapify",
                query=query,
                language=language,
                country_filter_code=country_filter_code,
                rows=cache_rows,
            )
        logger.info("geoapify api call query=%s results=%d cached", query, len(rows))

    items: list[ProviderPlace] = []
    for row in rows:
        item = feature_to_provider_place(row)
        if item is not None:
            items.append(item)
    return items


def row_to_provider_place(row: dict[str, Any]) -> ProviderPlace:
    return ProviderPlace(
        provider="geoapify",
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


def feature_to_provider_place(row: dict[str, Any]) -> ProviderPlace | None:
    lat = row.get("lat")
    lon = row.get("lon")
    if lat is None or lon is None:
        return None

    name = str(row.get("name") or row.get("formatted") or "").strip()
    if not name:
        return None

    locality = (
        row.get("city")
        or row.get("town")
        or row.get("village")
        or row.get("suburb")
        or row.get("state")
    )
    country = row.get("country")
    address = row.get("formatted")
    return ProviderPlace(
        provider="geoapify",
        provider_place_id=row.get("place_id"),
        name=name,
        subtitle=", ".join([part for part in [locality, country] if part]) or None,
        address=address,
        latitude=float(lat),
        longitude=float(lon),
        country=country,
        country_code=(row.get("country_code") or "").upper(),
        locality=locality,
        place_type=row.get("result_type"),
        category=row.get("result_type"),
    )


def normalize_geoapify_language(language: str) -> str:
    primary = language.split(",")[0].strip().lower()
    if not primary:
        return "en"

    normalized = primary.split("-")[0].split("_")[0].strip()
    return normalized or "en"
