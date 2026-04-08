from __future__ import annotations

import logging

import httpx

from app.ai_service import (
    call_deepseek,
    get_ai_token,
    infer_country_code,
    normalize_country_code,
    parse_itinerary_from_ai_output,
)
from app.ai_service_no_geocoding import build_parse_itinerary_no_geocoding_response
from app.config import settings
from app.schemas import (
    ActivityResponseSmart,
    DayPlanResponseSmart,
    ParseItinerarySmartResponse,
)

logger = logging.getLogger("tripcard-backend")


async def parse_itinerary_smart(
    text: str,
    destination: str | None,
    language: str,
) -> ParseItinerarySmartResponse:
    token_info = get_ai_token("deepseek")
    ai_output = await call_deepseek(text, destination, token_info)

    resolved_destination = ai_output.get("destination", destination or "")
    resolved_region = ai_output.get("region", resolved_destination)
    resolved_country = ai_output.get("country", "")
    resolved_country_code = normalize_country_code(ai_output.get("countryCode", ""))

    if not resolved_country_code:
        headers = {"User-Agent": settings.user_agent}
        timeout = httpx.Timeout(settings.request_timeout_seconds)
        async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
            resolved_country_code = await infer_country_code(
                client,
                language=language,
                country=resolved_country,
                destination=resolved_destination,
                region=resolved_region,
            )
        if resolved_country_code:
            ai_output["countryCode"] = resolved_country_code

    if resolved_country_code.upper() == "CN":
        response = build_parse_itinerary_no_geocoding_response(ai_output=ai_output, destination=destination)
        return ParseItinerarySmartResponse(
            destination=response.destination,
            totalDays=response.totalDays,
            summary=response.summary,
            dayPlans=[
                DayPlanResponseSmart(
                    id=day.id,
                    dayNumber=day.dayNumber,
                    date=day.date,
                    activities=[
                        ActivityResponseSmart(
                            id=activity.id,
                            title=activity.title,
                            searchName=activity.searchName,
                            category=activity.category,
                        location=None,
                        timeBucket=activity.timeBucket,
                        startTime=activity.startTime,
                        endTime=activity.endTime,
                        notes=activity.notes,
                    )
                        for activity in day.activities
                    ],
                    notes=day.notes,
                )
                for day in response.dayPlans
            ],
            warnings=response.warnings,
            geocodingMode="client_apple",
        )

    response = await parse_itinerary_from_ai_output(ai_output=ai_output, destination=destination, language=language)
    return ParseItinerarySmartResponse(
        destination=response.destination,
        totalDays=response.totalDays,
        summary=response.summary,
        dayPlans=[
            DayPlanResponseSmart(
                id=day.id,
                dayNumber=day.dayNumber,
                date=day.date,
                activities=[
                    ActivityResponseSmart(
                        id=activity.id,
                        title=activity.title,
                        searchName=None,
                        category=activity.category,
                        location=activity.location,
                        timeBucket=activity.timeBucket,
                        startTime=activity.startTime,
                        endTime=activity.endTime,
                        notes=activity.notes,
                    )
                    for activity in day.activities
                ],
                notes=day.notes,
            )
            for day in response.dayPlans
            ],
        warnings=response.warnings,
        geocodingMode="backend_geoapify",
    )
