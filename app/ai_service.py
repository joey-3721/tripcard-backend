from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import uuid

import httpx

from app.cache_mysql import MySQLCache
from app.config import settings
from app.providers.photon import search_photon
from app.schemas import (
    ActivityResponse,
    DayPlanResponse,
    ParseItineraryResponse,
    ParseItinerarySummaryResponse,
    TripLocationResponse,
)

logger = logging.getLogger("tripcard-backend")

SYSTEM_PROMPT = """\
You are a travel itinerary parser. The user will give you raw travel plan text \
(possibly in Chinese, English, or mixed). Extract a structured itinerary.

Return ONLY valid JSON, no markdown fences, no explanation. The JSON schema:

{
  "title": "string - concise trip card title in the original language, e.g. '巴黎48小时深度玩'",
  "destination": "string - primary destination city/region name in the original language",
  "country": "string - country name in the original language if confidently known, else empty string",
  "region": "string - city or region name in the original language if confidently known, else same as destination or empty string",
  "totalDays": number,
  "dayPlans": [
    {
      "dayNumber": 1,
      "activities": [
        {
          "title": "place display name in the original language",
          "searchName": "ENGLISH search query for geocoding, include city in English, e.g. 'Louvre Museum Paris France' or 'Eiffel Tower Paris' or 'Notre-Dame Cathedral Paris'",
          "category": "attraction|restaurant|hotel|transport|shopping|other",
          "timeBucket": "morning|noon|afternoon|evening|night|null",
          "startTime": "HH:MM or null",
          "endTime": "HH:MM or null",
          "notes": "any tips, costs, or details mentioned for this place",
          "cost": null,
          "currency": "EUR"
        }
      ],
      "notes": "general notes for this day"
    }
  ]
}

Rules:
- title should be short and suitable for a trip card
- category MUST be one of: attraction, restaurant, hotel, transport, shopping, other
- Activities MUST be in chronological order within each day
- searchName MUST be in English and include the city name for accurate geocoding
- If the text mentions 上午/中午/下午/傍晚/夜晚 or equivalent, fill timeBucket
- If explicit times like 09:30, 9点半, 14:00-16:00 are present, fill startTime/endTime in 24h HH:MM format
- If cost is mentioned with a currency symbol, extract both cost and currency
- Do NOT include transport between places as separate activities unless it is a notable activity (e.g. Seine river cruise)
- Merge nearby/related items into one activity if they are at the same location
"""


def get_ai_token(provider: str = "deepseek") -> dict:
    db = MySQLCache()
    row = db.get_ai_token(provider)
    if row is None:
        raise RuntimeError("No enabled AI token found")
    return row


async def call_deepseek(text: str, destination: str | None, token_info: dict) -> dict:
    user_content = ""
    if destination:
        user_content += f"Destination context: {destination}\n\n"
    user_content += f"Parse this itinerary:\n\n{text}"

    payload = {
        "model": token_info["model"],
        "temperature": 0.1,
        "max_tokens": 2048,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
    }

    base_url = token_info["base_url"].rstrip("/")
    headers = {
        "Authorization": f"Bearer {token_info['token']}",
        "Content-Type": "application/json",
    }

    timeout = httpx.Timeout(settings.ai_request_timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(f"{base_url}/v1/chat/completions", json=payload, headers=headers)
        resp.raise_for_status()

    data = resp.json()
    content = data["choices"][0]["message"]["content"]

    # Strip markdown fences if present
    content = re.sub(r"^```(?:json)?\s*", "", content.strip())
    content = re.sub(r"\s*```$", "", content.strip())

    return json.loads(content)


async def geocode_single_place(
    client: httpx.AsyncClient,
    search_name: str,
    destination: str | None,
    category: str,
    language: str,
    semaphore: asyncio.Semaphore,
) -> TripLocationResponse | None:
    async with semaphore:
        merged = []
        try:
            merged.extend(await search_photon(client, search_name, "en", None, 5))
        except Exception as exc:
            logger.warning("geocode photon failed query=%s error=%r", search_name, exc)

        if not merged:
            return None

        # Pick the best result: prefer one whose name contains the search query
        best = merged[0]
        for item in merged:
            if search_name.lower() in (item.name or "").lower():
                best = item
                break

        return TripLocationResponse(
            name=best.name,
            address=best.address or "",
            latitude=best.latitude,
            longitude=best.longitude,
            placeID=best.provider_place_id,
            country=best.country or "",
            countryCode=best.country_code or "",
            locality=best.locality or "",
            category=category if category in ("attraction", "restaurant", "hotel", "transport", "shopping", "other") else "other",
        )


async def parse_itinerary(text: str, destination: str | None, language: str) -> ParseItineraryResponse:
    token_info = get_ai_token("deepseek")

    # Check DeepSeek output cache first
    cache_key = hashlib.sha256(
        json.dumps({"cache_version": 2, "text": text, "destination": destination}, ensure_ascii=False, sort_keys=True).encode()
    ).hexdigest()
    db = MySQLCache()
    ai_output = db.get_ai_cache(cache_key)
    if ai_output is None:
        ai_output = await call_deepseek(text, destination, token_info)
        db.set_ai_cache(cache_key, ai_output)

    resolved_destination = ai_output.get("destination", destination or "")
    day_plans_raw = ai_output.get("dayPlans", [])
    warnings: list[str] = []

    # Collect all places that need geocoding
    geocode_tasks = []
    task_indices = []  # (day_idx, activity_idx)

    for day_idx, day in enumerate(day_plans_raw):
        for act_idx, act in enumerate(day.get("activities", [])):
            search_name = act.get("searchName") or act.get("title", "")
            category = act.get("category", "other")
            if search_name:
                geocode_tasks.append((search_name, category))
                task_indices.append((day_idx, act_idx))

    # Geocode all places concurrently
    semaphore = asyncio.Semaphore(settings.ai_geocode_concurrency)
    headers = {"User-Agent": settings.user_agent}
    timeout = httpx.Timeout(settings.request_timeout_seconds)

    async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
        coros = [
            geocode_single_place(client, name, resolved_destination, cat, language, semaphore)
            for name, cat in geocode_tasks
        ]
        locations = await asyncio.gather(*coros, return_exceptions=True)

    # Map geocoded locations back
    location_map: dict[tuple[int, int], TripLocationResponse | None] = {}
    for idx, loc in enumerate(locations):
        day_idx, act_idx = task_indices[idx]
        if isinstance(loc, Exception):
            logger.warning("geocode failed for task %s: %r", geocode_tasks[idx][0], loc)
            warnings.append(f"Failed to geocode: {geocode_tasks[idx][0]}")
            location_map[(day_idx, act_idx)] = None
        else:
            location_map[(day_idx, act_idx)] = loc
            if loc is None:
                warnings.append(f"No location found: {geocode_tasks[idx][0]}")

    # Build response
    day_plans: list[DayPlanResponse] = []
    geocoded_locations: list[TripLocationResponse] = []
    for day_idx, day in enumerate(day_plans_raw):
        activities: list[ActivityResponse] = []
        for act_idx, act in enumerate(day.get("activities", [])):
            location = location_map.get((day_idx, act_idx))
            # If no geocoded location, create a stub with the title
            if location is None and act.get("title"):
                location = TripLocationResponse(
                    name=act.get("searchName") or act["title"],
                    country="",
                    countryCode="",
                    locality="",
                    category=act.get("category", "other"),
                )
            elif location is not None:
                geocoded_locations.append(location)

            activities.append(ActivityResponse(
                id=str(uuid.uuid4()),
                title=act.get("title", ""),
                category=act.get("category", "other"),
                location=location,
                timeBucket=infer_time_bucket(act),
                startTime=normalize_time_value(act.get("startTime")) or infer_time_range(act.get("notes", ""))[0],
                endTime=normalize_time_value(act.get("endTime")) or infer_time_range(act.get("notes", ""))[1],
                notes=act.get("notes", ""),
                cost=act.get("cost"),
                currency=act.get("currency", "CNY"),
            ))

        day_plans.append(DayPlanResponse(
            id=str(uuid.uuid4()),
            dayNumber=day.get("dayNumber", day_idx + 1),
            activities=activities,
            notes=day.get("notes", ""),
        ))

    total_days = ai_output.get("totalDays", len(day_plans))
    summary = build_summary(
        ai_output=ai_output,
        resolved_destination=resolved_destination,
        total_days=total_days,
        geocoded_locations=geocoded_locations,
    )

    return ParseItineraryResponse(
        destination=resolved_destination,
        totalDays=total_days,
        summary=summary,
        dayPlans=day_plans,
        rawAiOutput=ai_output,
        warnings=warnings,
    )


def build_summary(
    ai_output: dict,
    resolved_destination: str,
    total_days: int,
    geocoded_locations: list[TripLocationResponse],
) -> ParseItinerarySummaryResponse:
    title = str(ai_output.get("title") or "").strip()
    country = str(ai_output.get("country") or "").strip()
    region = str(ai_output.get("region") or "").strip()

    country_code_votes: dict[str, int] = {}
    country_name_votes: dict[str, int] = {}
    locality_votes: dict[str, int] = {}

    for location in geocoded_locations:
        if location.countryCode:
            country_code_votes[location.countryCode] = country_code_votes.get(location.countryCode, 0) + 1
        if location.country:
            country_name_votes[location.country] = country_name_votes.get(location.country, 0) + 1
        if location.locality:
            locality_votes[location.locality] = locality_votes.get(location.locality, 0) + 1
        if location.address:
            address_parts = [part.strip() for part in location.address.split(",") if part.strip()]
            if address_parts and not country:
                country_name_votes[address_parts[-1]] = country_name_votes.get(address_parts[-1], 0) + 1

    inferred_country = most_common_value(country_name_votes)
    inferred_country_code = most_common_value(country_code_votes)
    inferred_region = most_common_value(locality_votes)

    if not country:
        country = inferred_country or ""
    if not region:
        region = inferred_region or resolved_destination
    if not title:
        duration_suffix = f"{total_days}天行程" if total_days > 0 else "行程"
        title = f"{resolved_destination}{duration_suffix}" if resolved_destination else duration_suffix

    return ParseItinerarySummaryResponse(
        title=title,
        destination=resolved_destination,
        country=country,
        countryCode=inferred_country_code or "",
        region=region,
        totalDays=total_days,
    )


def most_common_value(counts: dict[str, int]) -> str | None:
    if not counts:
        return None
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def infer_time_bucket(activity: dict) -> str | None:
    direct = normalize_time_bucket(activity.get("timeBucket"))
    if direct:
        return direct

    texts = [
        str(activity.get("title") or ""),
        str(activity.get("notes") or ""),
    ]
    for text in texts:
        bucket = normalize_time_bucket(text)
        if bucket:
            return bucket
    return None


def normalize_time_bucket(value: str | None) -> str | None:
    if not value:
        return None

    text = str(value).strip().lower()
    mappings = {
        "morning": "morning",
        "上午": "morning",
        "早上": "morning",
        "清晨": "morning",
        "中午": "noon",
        "noon": "noon",
        "午间": "noon",
        "afternoon": "afternoon",
        "下午": "afternoon",
        "傍晚": "evening",
        "evening": "evening",
        "黄昏": "evening",
        "night": "night",
        "夜晚": "night",
        "晚上": "night",
        "夜间": "night",
    }
    for token, normalized in mappings.items():
        if token in text:
            return normalized
    return None


def infer_time_range(text: str) -> tuple[str | None, str | None]:
    normalized = str(text or "").strip()
    if not normalized:
        return None, None

    explicit = [
        normalize_time_value(match)
        for match in re.findall(r"(?<!\d)(?:[01]?\d|2[0-3])[:：点时](?:[0-5]\d|半)?", normalized)
    ]
    explicit = [item for item in explicit if item]
    if len(explicit) >= 2:
        return explicit[0], explicit[1]
    if len(explicit) == 1:
        return explicit[0], None
    return None, None


def normalize_time_value(value: str | None) -> str | None:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    match = re.search(r"^(?P<hour>\d{1,2})(?:[:：点时](?P<minute>\d{1,2}|半)?)?$", text)
    if not match:
        return None

    hour = int(match.group("hour"))
    minute_token = match.group("minute")
    minute = 0
    if minute_token:
        minute = 30 if minute_token == "半" else int(minute_token)

    if hour > 23 or minute > 59:
        return None
    return f"{hour:02d}:{minute:02d}"
