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


# ── AI Itinerary Parsing ──

ActivityCategoryType = Literal[
    "attraction", "restaurant", "hotel", "transport", "shopping", "other"
]


class ParseItineraryRequest(BaseModel):
    text: str = Field(..., min_length=10, max_length=10000)
    destination: str | None = Field(
        default=None,
        description="Primary destination hint, e.g. '巴黎' or 'Paris, France'",
    )
    language: str = "zh-CN"


class TripLocationResponse(BaseModel):
    name: str
    address: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    placeID: str | None = None
    category: ActivityCategoryType = "other"


class ActivityResponse(BaseModel):
    id: str
    title: str
    category: ActivityCategoryType = "other"
    location: TripLocationResponse | None = None
    startTime: str | None = None
    endTime: str | None = None
    notes: str = ""
    cost: float | None = None
    currency: str = "CNY"


class DayPlanResponse(BaseModel):
    id: str
    dayNumber: int
    date: str | None = None
    activities: list[ActivityResponse]
    notes: str = ""


class ParseItineraryResponse(BaseModel):
    destination: str
    totalDays: int
    dayPlans: list[DayPlanResponse]
    rawAiOutput: dict | None = None
    warnings: list[str] = Field(default_factory=list)
