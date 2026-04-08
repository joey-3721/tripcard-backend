from __future__ import annotations

import hashlib
import json
import logging
import uuid

from app.ai_service import (
    SYSTEM_PROMPT,
    get_ai_token,
    call_deepseek,
    normalize_day_plans_raw,
    normalize_country_code,
    standardized_activity_title,
    infer_time_bucket,
    normalize_time_value,
    infer_time_range,
)
from app.cache_mysql import MySQLCache
from app.schemas import (
    ActivityResponseNoLocation,
    DayPlanResponseNoLocation,
    ParseItinerarySummaryResponse,
    ParseItineraryResponseNoLocation,
)

logger = logging.getLogger("tripcard-backend")


async def parse_itinerary_no_geocoding(
    text: str, destination: str | None, language: str, use_cache: bool = False
) -> ParseItineraryResponseNoLocation:
    """Parse itinerary without backend geocoding - returns search queries for client-side geocoding"""
    token_info = get_ai_token("deepseek")

    # Check DeepSeek output cache (optional)
    ai_output = None
    if use_cache:
        cache_key = hashlib.sha256(
            json.dumps(
                {"cache_version": 8, "text": text, "destination": destination},
                ensure_ascii=False,
                sort_keys=True,
            ).encode()
        ).hexdigest()
        db = MySQLCache()
        ai_output = db.get_ai_cache(cache_key)
        if ai_output:
            logger.info("ai parse cache hit destination=%s", destination or "")

    if ai_output is None:
        ai_output = await call_deepseek(text, destination, token_info)
        if use_cache:
            db = MySQLCache()
            db.set_ai_cache(cache_key, ai_output)
        logger.info("ai parse cache miss destination=%s", destination or "")

    resolved_destination = ai_output.get("destination", destination or "")
    resolved_region = ai_output.get("region", resolved_destination)
    resolved_country = ai_output.get("country", "")
    resolved_country_code = normalize_country_code(ai_output.get("countryCode", ""))
    day_plans_raw = normalize_day_plans_raw(ai_output.get("dayPlans", []))

    # Build response without geocoding
    day_plans: list[DayPlanResponseNoLocation] = []
    for day_idx, day in enumerate(day_plans_raw):
        activities: list[ActivityResponseNoLocation] = []
        for act in day.get("activities", []):
            title = standardized_activity_title(act)
            search_name = act.get("searchName") or title

            # For China, use Chinese title as search query
            if resolved_country_code and resolved_country_code.upper() == "CN":
                search_query = title  # Use Chinese
            else:
                search_query = search_name  # Use English

            activities.append(
                ActivityResponseNoLocation(
                    id=str(uuid.uuid4()),
                    title=title,
                    searchName=search_query,
                    category=act.get("category", "other"),
                    timeBucket=infer_time_bucket(act),
                    startTime=normalize_time_value(act.get("startTime"))
                    or infer_time_range(act.get("notes") or "")[0],
                    endTime=normalize_time_value(act.get("endTime"))
                    or infer_time_range(act.get("notes") or "")[1],
                    notes=act.get("notes") or "",
                    cost=act.get("cost"),
                    currency=act.get("currency"),
                )
            )

        day_plans.append(
            DayPlanResponseNoLocation(
                id=str(uuid.uuid4()),
                dayNumber=day.get("dayNumber", day_idx + 1),
                activities=activities,
                notes=day.get("notes") or "",
            )
        )

    return ParseItineraryResponseNoLocation(
        destination=resolved_destination,
        totalDays=len(day_plans_raw),
        summary=ParseItinerarySummaryResponse(
            title=ai_output.get("title", ""),
            destination=resolved_destination,
            country=resolved_country,
            countryCode=resolved_country_code,
            region=resolved_region,
            totalDays=len(day_plans_raw),
        ),
        dayPlans=day_plans,
        rawAiOutput=ai_output,
        warnings=[],
    )
