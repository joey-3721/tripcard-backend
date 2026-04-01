from __future__ import annotations

from typing import Any
import httpx

from app.config import settings
from app.providers.base import ProviderPlace


async def search_nominatim(
    client: httpx.AsyncClient,
    query: str,
    language: str,
    country_filter_code: str | None,
    limit: int,
) -> list[ProviderPlace]:
    params = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": max(limit, 12),
        "accept-language": language,
    }
    if country_filter_code:
        params["countrycodes"] = country_filter_code.lower()

    response = await client.get(settings.nominatim_base_url, params=params)
    response.raise_for_status()
    payload: list[dict[str, Any]] = response.json()

    items: list[ProviderPlace] = []
    for row in payload:
        lat = row.get("lat")
        lon = row.get("lon")
        if lat is None or lon is None:
            continue

        address = row.get("address") or {}
        country = address.get("country")
        country_code = (address.get("country_code") or "").upper()
        locality = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("municipality")
            or address.get("county")
            or address.get("state")
        )
        name = (row.get("name") or str(row.get("display_name", "")).split(",")[0]).strip()
        if not name:
            continue

        items.append(
            ProviderPlace(
                provider="nominatim",
                provider_place_id=str(row.get("place_id")) if row.get("place_id") is not None else None,
                name=name,
                subtitle=", ".join([part for part in [locality, country] if part]) or None,
                address=row.get("display_name"),
                latitude=float(lat),
                longitude=float(lon),
                country=country,
                country_code=country_code,
                locality=locality,
                place_type=row.get("type") or row.get("addresstype"),
            )
        )
    return items
