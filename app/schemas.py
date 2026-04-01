from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field


SearchScope = Literal["all", "domestic", "international"]
SearchCategory = Literal["hotel", "attraction", "restaurant", "shopping", "transport", "other"]


class DestinationSeed(BaseModel):
    name: str
    country: str
    country_code: str = Field(default="")


class DestinationContext(BaseModel):
    trip_id: str | None = None
    destinations: list[DestinationSeed] = Field(default_factory=list)


class CoordinatePayload(BaseModel):
    latitude: float
    longitude: float


class PlaceSearchRequest(BaseModel):
    query: str
    category: SearchCategory = "other"
    scope: SearchScope = "all"
    preferred_country_codes: list[str] = Field(default_factory=list)
    country_filter_code: str | None = None
    destination_context: DestinationContext | None = None
    user_location: CoordinatePayload | None = None
    language: str = "zh-CN"
    limit: int = 12


class PlaceResult(BaseModel):
    id: str
    name: str
    subtitle: str | None = None
    address: str | None = None
    coordinate: CoordinatePayload
    country: str | None = None
    country_code: str = ""
    locality: str | None = None
    place_type: str | None = None
    category: str | None = None
    provider: str | None = None
    provider_place_id: str | None = None
    score: float | None = None
    matched_by: list[str] = Field(default_factory=list)


class PlaceSearchMeta(BaseModel):
    scope: str | None = None
    country_filter_code: str | None = None
    preferred_country_codes: list[str] = Field(default_factory=list)
    fallback_used: bool = False
    providers_used: list[str] = Field(default_factory=list)
    cache_hit: bool = False
    self_hosted_data: bool = False


class PlaceSearchResponse(BaseModel):
    query: str
    trace_id: str
    results: list[PlaceResult]
    meta: PlaceSearchMeta
