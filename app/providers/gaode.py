from __future__ import annotations

from typing import Any
import httpx
import logging

from app.cache_mysql import MySQLCache
from app.providers.base import ProviderPlace

logger = logging.getLogger("tripcard-backend")


async def search_gaode(
    client: httpx.AsyncClient,
    query: str,
    language: str,
    country_filter_code: str | None,
    limit: int,
) -> list[ProviderPlace]:
    """Search using Gaode (高德) Maps API for China locations."""

    # Only use Gaode for China queries - must have CN filter
    if not country_filter_code or country_filter_code.upper() != "CN":
        return []

    db = MySQLCache()

    # Check cache first
    cached_results = db.get_place_geocode_cache(
        source="gaode",
        query=query,
        language=language,
        country_filter_code=country_filter_code,
        limit=limit,
    )
    if cached_results:
        logger.info("gaode cache hit query=%s results=%d", query, len(cached_results))
        items: list[ProviderPlace] = []
        for row in cached_results:
            items.append(
                ProviderPlace(
                    provider="gaode",
                    provider_place_id=row.get("poi_id"),
                    name=row["poi_name"],
                    subtitle=", ".join([p for p in [row.get("city"), row.get("district")] if p]) or None,
                    address="".join([p for p in [row.get("province"), row.get("city"), row.get("district"), row.get("address")] if p]) or None,
                    latitude=float(row["latitude"]),
                    longitude=float(row["longitude"]),
                    country="中国",
                    country_code="CN",
                    locality=row.get("city") or row.get("district") or row.get("province"),
                    place_type=row.get("poi_type"),
                    category=row.get("poi_typecode"),
                )
            )
        return items

    # Get Gaode API key from database
    token_row = db.get_ai_token("gaode")
    if token_row is None:
        logger.warning("gaode token not found in database")
        return []

    api_key = token_row["token"]

    params = {
        "key": api_key,
        "keywords": query,
        "output": "json",
        "offset": min(limit, 20),
    }

    # Gaode Geocoding API endpoint
    url = "https://restapi.amap.com/v3/place/text"

    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        if data.get("status") != "1":
            logger.warning("gaode api error query=%s status=%s", query, data.get("status"))
            return []

        pois = data.get("pois", [])

        # Cache the results
        if pois:
            cache_rows: list[dict[str, Any]] = []
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

                province = poi.get("pname", "")
                city = poi.get("cityname", "")
                district = poi.get("adname", "")
                address = poi.get("address", "")
                locality = city or district or province
                subtitle = ", ".join([p for p in [city, district] if p]) or None
                full_address = "".join([p for p in [province, city, district, address] if p]) or None

                cache_rows.append(
                    {
                        "place_id": poi.get("id"),
                        "name": poi.get("name", "").strip(),
                        "address": full_address,
                        "subtitle": subtitle,
                        "latitude": latitude,
                        "longitude": longitude,
                        "country": "中国",
                        "country_code": "CN",
                        "locality": locality,
                        "place_type": poi.get("type"),
                        "category": poi.get("typecode"),
                        "full_response": poi,
                    }
                )
            db.set_place_geocode_cache(
                source="gaode",
                query=query,
                language=language,
                country_filter_code=country_filter_code,
                rows=cache_rows,
            )
            logger.info("gaode api call query=%s results=%d cached", query, len(pois))

        items: list[ProviderPlace] = []

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

            name = poi.get("name", "").strip()
            if not name:
                continue

            # Extract address components
            province = poi.get("pname", "")
            city = poi.get("cityname", "")
            district = poi.get("adname", "")
            address = poi.get("address", "")

            # Build full address
            full_address_parts = [province, city, district, address]
            full_address = "".join([p for p in full_address_parts if p])

            # Determine locality (prefer city, fallback to district or province)
            locality = city or district or province

            # Build subtitle
            subtitle_parts = [p for p in [city, district] if p]
            subtitle = ", ".join(subtitle_parts) if subtitle_parts else None

            items.append(
                ProviderPlace(
                    provider="gaode",
                    provider_place_id=poi.get("id"),
                    name=name,
                    subtitle=subtitle,
                    address=full_address or None,
                    latitude=latitude,
                    longitude=longitude,
                    country="中国",
                    country_code="CN",
                    locality=locality,
                    place_type=poi.get("type"),
                    category=poi.get("typecode"),
                )
            )

        return items

    except Exception as exc:
        logger.warning("gaode api exception query=%s error=%r", query, exc)
        return []
