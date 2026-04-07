from __future__ import annotations

from typing import Any
import httpx

from app.config import settings
from app.providers.base import ProviderPlace

SUPPORTED_PHOTON_LANGUAGES = {"default", "de", "en", "fr"}


async def search_photon(
    client: httpx.AsyncClient,
    query: str,
    language: str,
    country_filter_code: str | None,
    limit: int,
) -> list[ProviderPlace]:
    params = {
        "q": query,
        "limit": max(limit, 12),
    }

    lang = normalize_photon_language(language)
    if lang != "default":
        params["lang"] = lang

    response = await client.get(settings.photon_base_url, params=params)
    response.raise_for_status()
    payload: dict[str, Any] = response.json()

    items: list[ProviderPlace] = []
    for feature in payload.get("features", []):
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") or []
        if len(coordinates) != 2:
            continue

        properties = feature.get("properties") or {}
        name = (properties.get("name") or properties.get("street") or "").strip()
        if not name:
            continue

        country = properties.get("country")
        country_code = (properties.get("countrycode") or "").upper()
        locality = (
            properties.get("city")
            or properties.get("county")
            or properties.get("state")
        )

        if country_filter_code and country_code and country_code != country_filter_code.upper():
            continue

        items.append(
            ProviderPlace(
                provider="photon",
                provider_place_id=str(properties.get("osm_id")) if properties.get("osm_id") is not None else None,
                name=name,
                subtitle=", ".join([part for part in [locality, country] if part]) or None,
                address=", ".join(
                    [
                        part
                        for part in [
                            properties.get("street"),
                            properties.get("postcode"),
                            locality,
                            country,
                        ]
                        if part
                    ]
                ) or None,
                latitude=float(coordinates[1]),
                longitude=float(coordinates[0]),
                country=country,
                country_code=country_code,
                locality=locality,
                place_type=properties.get("osm_value") or properties.get("type"),
            )
        )
    return items


def normalize_photon_language(language: str) -> str:
    primary = language.split(",")[0].strip().lower()
    if not primary:
        return "default"

    candidate = primary.split("-")[0].split("_")[0].strip()
    if candidate in SUPPORTED_PHOTON_LANGUAGES:
        return candidate

    if primary in SUPPORTED_PHOTON_LANGUAGES:
        return primary

    return "default"
