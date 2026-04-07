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
    TripLocationResponse,
)

logger = logging.getLogger("tripcard-backend")

SYSTEM_PROMPT = """\
You are a travel itinerary parser. The user will give you raw travel plan text \
(possibly in Chinese, English, or mixed). Extract a structured itinerary.

Return ONLY valid JSON, no markdown fences, no explanation. The JSON schema:

{
  "destination": "string - primary destination city/region name in the original language",
  "totalDays": number,
  "dayPlans": [
    {
      "dayNumber": 1,
      "activities": [
        {
          "title": "place display name in the original language",
          "searchName": "ENGLISH search query for geocoding, include city in English, e.g. 'Louvre Museum Paris France' or 'Eiffel Tower Paris' or 'Notre-Dame Cathedral Paris'",
          "category": "attraction|restaurant|hotel|transport|shopping|other",
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
- category MUST be one of: attraction, restaurant, hotel, transport, shopping, other
- Activities MUST be in chronological order within each day
- searchName MUST be in English and include the city name for accurate geocoding
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
            category=category if category in ("attraction", "restaurant", "hotel", "transport", "shopping", "other") else "other",
        )


async def parse_itinerary(text: str, destination: str | None, language: str) -> ParseItineraryResponse:
    token_info = get_ai_token("deepseek")

    # Check DeepSeek output cache first
    cache_key = hashlib.sha256(
        json.dumps({"text": text, "destination": destination}, ensure_ascii=False, sort_keys=True).encode()
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
    for day_idx, day in enumerate(day_plans_raw):
        activities: list[ActivityResponse] = []
        for act_idx, act in enumerate(day.get("activities", [])):
            location = location_map.get((day_idx, act_idx))
            # If no geocoded location, create a stub with the title
            if location is None and act.get("title"):
                location = TripLocationResponse(
                    name=act.get("searchName") or act["title"],
                    category=act.get("category", "other"),
                )

            activities.append(ActivityResponse(
                id=str(uuid.uuid4()),
                title=act.get("title", ""),
                category=act.get("category", "other"),
                location=location,
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

    return ParseItineraryResponse(
        destination=resolved_destination,
        totalDays=ai_output.get("totalDays", len(day_plans)),
        dayPlans=day_plans,
        rawAiOutput=ai_output,
        warnings=warnings,
    )
