from __future__ import annotations

import hashlib
import json
import logging
import uuid

from app.ai_service import (
    get_ai_token,
    call_ai_model,
    normalize_day_plans_raw,
    normalize_country_code,
    normalize_ai_provider,
    normalize_summary_region,
    normalized_summary_title,
    sanitize_activity_notes,
    standardized_activity_title,
    infer_time_bucket,
    normalize_time_value,
    sanitize_day_notes,
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
    text: str,
    destination: str | None,
    language: str,
    model_name: str = "deepseek",
    use_cache: bool = False,
) -> ParseItineraryResponseNoLocation:
    """Parse itinerary without backend geocoding - returns search queries for client-side geocoding"""
    provider = normalize_ai_provider(model_name)
    token_info = get_ai_token(provider)

    # Check AI output cache (optional)
    ai_output = None
    if use_cache:
        cache_key = hashlib.sha256(
            json.dumps(
                {"cache_version": 9, "text": text, "destination": destination, "provider": provider},
                ensure_ascii=False,
                sort_keys=True,
            ).encode()
        ).hexdigest()
        db = MySQLCache()
        ai_output = db.get_ai_cache(cache_key)
        if ai_output:
            logger.info("ai parse cache hit provider=%s destination=%s", provider, destination or "")

    if ai_output is None:
        ai_output = await call_ai_model(text, destination, token_info, provider)
        if use_cache:
            db = MySQLCache()
            db.set_ai_cache(cache_key, ai_output)
        logger.info("ai parse cache miss provider=%s destination=%s", provider, destination or "")

    response = build_parse_itinerary_no_geocoding_response(
        ai_output=ai_output,
        destination=destination,
    )
    logger.info(
        "parse-itinerary-no-geocoding frontend response provider=%s payload=%s",
        provider,
        json.dumps(response.model_dump(mode="json"), ensure_ascii=False),
    )
    return response


def build_parse_itinerary_no_geocoding_response(
    ai_output: dict,
    destination: str | None,
) -> ParseItineraryResponseNoLocation:
    resolved_destination = ai_output.get("destination", destination or "")
    resolved_region = ai_output.get("region", resolved_destination)
    resolved_country = ai_output.get("country", "")
    resolved_country_code = normalize_country_code(ai_output.get("countryCode", ""))
    resolved_region = normalize_summary_region(
        destination=resolved_destination,
        region=resolved_region,
        country=resolved_country,
        country_code=resolved_country_code,
    )
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
                    timeBucket=None,
                    startTime=None,
                    endTime=None,
                    notes="",
                )
            )

        day_plans.append(
            DayPlanResponseNoLocation(
                id=str(uuid.uuid4()),
                dayNumber=day.get("dayNumber", day_idx + 1),
                activities=activities,
                notes="",
            )
        )

    return ParseItineraryResponseNoLocation(
        destination=resolved_destination,
        totalDays=len(day_plans_raw),
        summary=ParseItinerarySummaryResponse(
            title=normalized_summary_title(
                raw_title=str(ai_output.get("title") or ""),
                destination=resolved_destination,
                total_days=len(day_plans_raw),
                day_plans_raw=day_plans_raw,
            ),
            destination=resolved_destination,
            country=resolved_country,
            countryCode=resolved_country_code,
            region=resolved_region,
            totalDays=len(day_plans_raw),
        ),
        dayPlans=day_plans,
        warnings=[],
    )
