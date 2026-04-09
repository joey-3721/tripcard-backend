from __future__ import annotations

from typing import Literal
from pydantic import BaseModel, Field


SearchScope = Literal["all", "domestic", "international"]
SearchCategory = Literal["hotel", "attraction", "restaurant", "shopping", "transport", "other"]
ItineraryGeocodingMode = Literal["client_apple", "backend_geoapify"]
AIParseJobStatusType = Literal["queued", "processing", "completed", "failed"]


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
ActivityTimeBucketType = Literal[
    "morning", "noon", "afternoon", "evening", "night"
]


class ParseItineraryRequest(BaseModel):
    text: str = Field(..., min_length=10, max_length=10000)
    destination: str | None = Field(
        default=None,
        description="Primary destination hint, e.g. '巴黎' or 'Paris, France'",
    )
    language: str = "zh-CN"
    modelName: str = Field(
        default="deepseek",
        description="AI model selector from client, e.g. 'deepseek' or 'qwen-turbo'",
    )


class TripLocationResponse(BaseModel):
    name: str
    address: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    placeID: str | None = None
    country: str = ""
    countryCode: str = ""
    locality: str = ""
    category: ActivityCategoryType = "other"


class ActivityResponse(BaseModel):
    id: str
    title: str
    category: ActivityCategoryType = "other"
    location: TripLocationResponse | None = None
    timeBucket: ActivityTimeBucketType | None = None
    startTime: str | None = None
    endTime: str | None = None
    notes: str = ""


class DayPlanResponse(BaseModel):
    id: str
    dayNumber: int
    date: str | None = None
    activities: list[ActivityResponse]
    notes: str = ""


class ParseItinerarySummaryResponse(BaseModel):
    title: str = ""
    destination: str = ""
    country: str = ""
    countryCode: str = ""
    region: str = ""
    totalDays: int = 0


class ParseItineraryResponse(BaseModel):
    destination: str
    totalDays: int
    summary: ParseItinerarySummaryResponse | None = None
    dayPlans: list[DayPlanResponse]
    warnings: list[str] = Field(default_factory=list)


# ── AI Itinerary Parsing (Client-side Geocoding) ──

class ActivityResponseNoLocation(BaseModel):
    """Activity without geocoded location - for client-side geocoding"""
    id: str
    title: str
    searchName: str  # Query string for client to search
    category: ActivityCategoryType = "other"
    timeBucket: ActivityTimeBucketType | None = None
    startTime: str | None = None
    endTime: str | None = None
    notes: str = ""


class DayPlanResponseNoLocation(BaseModel):
    """Day plan without geocoded locations"""
    id: str
    dayNumber: int
    date: str | None = None
    activities: list[ActivityResponseNoLocation]
    notes: str = ""


class ParseItineraryResponseNoLocation(BaseModel):
    """Itinerary parsing response without backend geocoding - for client-side geocoding"""
    destination: str
    totalDays: int
    summary: ParseItinerarySummaryResponse | None = None
    dayPlans: list[DayPlanResponseNoLocation]
    warnings: list[str] = Field(default_factory=list)


class ActivityResponseSmart(BaseModel):
    id: str
    title: str
    searchName: str | None = None
    category: ActivityCategoryType = "other"
    location: TripLocationResponse | None = None
    timeBucket: ActivityTimeBucketType | None = None
    startTime: str | None = None
    endTime: str | None = None
    notes: str = ""


class DayPlanResponseSmart(BaseModel):
    id: str
    dayNumber: int
    date: str | None = None
    activities: list[ActivityResponseSmart]
    notes: str = ""


class ParseItinerarySmartResponse(BaseModel):
    destination: str
    totalDays: int
    summary: ParseItinerarySummaryResponse | None = None
    dayPlans: list[DayPlanResponseSmart]
    warnings: list[str] = Field(default_factory=list)
    geocodingMode: ItineraryGeocodingMode


class ParseItineraryAsyncStartResponse(BaseModel):
    taskId: str | None = None
    status: AIParseJobStatusType
    progress: int = 0
    message: str = ""
    cacheHit: bool = False
    result: ParseItinerarySmartResponse | None = None


class ParseItineraryAsyncStatusResponse(BaseModel):
    taskId: str
    status: AIParseJobStatusType
    progress: int = 0
    message: str = ""
    result: ParseItinerarySmartResponse | None = None
    error: str | None = None
