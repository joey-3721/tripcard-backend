from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class ProviderPlace:
    provider: str
    provider_place_id: str | None
    name: str
    subtitle: str | None
    address: str | None
    latitude: float
    longitude: float
    country: str | None = None
    country_code: str = ""
    locality: str | None = None
    place_type: str | None = None
    category: str | None = None
    matched_by: list[str] = field(default_factory=list)
