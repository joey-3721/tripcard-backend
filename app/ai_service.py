from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import uuid

import httpx

from app.cache_mysql import MySQLCache
from app.config import settings
from app.providers.nominatim import search_nominatim
from app.providers.photon import search_photon
from app.schemas import (
    ActivityResponse,
    DayPlanResponse,
    DestinationContext,
    DestinationSeed,
    ParseItineraryResponse,
    ParseItinerarySummaryResponse,
    PlaceSearchRequest,
    TripLocationResponse,
)

logger = logging.getLogger("tripcard-backend")
MAX_DAY_OUTLIER_DISTANCE_KM = 120.0
MAX_TRIP_OUTLIER_DISTANCE_KM = 180.0

LANDMARK_QUERY_REWRITES: dict[str, tuple[str, ...]] = {
    "bird s nest": (
        "National Stadium Beijing China",
        "国家体育场",
        "Olympic Green Beijing National Stadium",
    ),
    "鸟巢": (
        "National Stadium Beijing China",
        "国家体育场",
    ),
    "water cube": (
        "National Aquatics Center Beijing China",
        "国家游泳中心",
        "Water Cube Beijing Olympic Park",
    ),
    "水立方": (
        "National Aquatics Center Beijing China",
        "国家游泳中心",
    ),
    "sanlitun": (
        "Sanlitun Taikoo Li Beijing China",
        "三里屯太古里",
        "Sanlitun Beijing China",
    ),
    "三里屯": (
        "Sanlitun Taikoo Li Beijing China",
        "三里屯太古里",
    ),
    "shichahai": (
        "Shichahai Scenic Area Beijing China",
        "什刹海风景区",
        "Shichahai Beijing China",
    ),
    "什刹海": (
        "Shichahai Scenic Area Beijing China",
        "什刹海风景区",
    ),
    "prince gong s mansion": (
        "Prince Gong Mansion Beijing China",
        "恭王府",
    ),
    "恭王府": (
        "Prince Gong Mansion Beijing China",
        "恭王府",
    ),
}

ATTRACTION_RESULT_REJECT_TOKENS = (
    "hotel",
    "hostel",
    "inn",
    "guesthouse",
    "apartment",
    "road",
    "street",
    "highway",
    "metro",
    "subway",
    "line",
    "station",
    "book mansion",
    "grand hotel",
    "express hotel",
    "mansion beijing",
)

CANONICAL_ACTIVITY_RULES: dict[str, dict] = {
    "国博": {
        "title": "中国国家博物馆",
        "search_name": "National Museum of China Beijing China",
        "acceptable": ("中国国家博物馆", "national museum of china"),
    },
    "故宫": {
        "title": "故宫博物院",
        "search_name": "Palace Museum Beijing China",
        "acceptable": ("故宫博物院", "故宫", "palace museum", "forbidden city"),
    },
    "天安门": {
        "title": "天安门广场",
        "search_name": "Tiananmen Square Beijing China",
        "acceptable": ("天安门广场", "天安门", "tiananmen square"),
    },
    "天坛": {
        "title": "天坛公园",
        "search_name": "Temple of Heaven Beijing China",
        "acceptable": ("天坛公园", "天坛", "temple of heaven"),
    },
    "鸟巢": {
        "title": "国家体育场",
        "search_name": "National Stadium Beijing China",
        "acceptable": ("国家体育场", "national stadium", "bird s nest"),
    },
    "水立方": {
        "title": "国家游泳中心",
        "search_name": "National Aquatics Center Beijing China",
        "acceptable": ("国家游泳中心", "national aquatics center", "water cube"),
    },
    "什刹海": {
        "title": "什刹海风景区",
        "search_name": "Shichahai Scenic Area Beijing China",
        "acceptable": ("什刹海", "什刹海风景区", "shichahai"),
    },
    "恭王府": {
        "title": "恭王府",
        "search_name": "Prince Gong Mansion Beijing China",
        "acceptable": ("恭王府", "prince gong mansion"),
    },
    "北海": {
        "title": "北海公园",
        "search_name": "Beihai Park Beijing China",
        "acceptable": ("北海公园", "beihai park"),
    },
    "景山": {
        "title": "景山公园",
        "search_name": "Jingshan Park Beijing China",
        "acceptable": ("景山公园", "jingshan park"),
    },
    "三里屯": {
        "title": "三里屯太古里",
        "search_name": "Sanlitun Taikoo Li Beijing China",
        "acceptable": ("三里屯", "三里屯太古里", "sanlitun taikoo li", "sanlitun"),
    },
    "环球影城": {
        "title": "北京环球影城",
        "search_name": "Universal Beijing Resort China",
        "acceptable": ("北京环球影城", "universal beijing resort", "universal studios beijing"),
    },
    "清北": {
        "split": (
            {
                "title": "清华大学",
                "searchName": "Tsinghua University Beijing China",
                "category": "attraction",
            },
            {
                "title": "北京大学",
                "searchName": "Peking University Beijing China",
                "category": "attraction",
            },
        ),
    },
}

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
          "title": "standard full place name in the original language, NOT a shorthand, e.g. '中国国家博物馆' instead of '国博'",
          "originalMention": "the original mention from user text, may be shorthand like '国博' or '清北'",
          "canonicalTitle": "same as title unless you must preserve a display nuance",
          "searchName": "ENGLISH canonical search query for geocoding, include city in English, e.g. 'Louvre Museum Paris France' or 'Eiffel Tower Paris' or 'Notre-Dame Cathedral Paris'",
          "category": "attraction|restaurant|hotel|transport|shopping|other",
          "timeBucket": "morning|noon|afternoon|evening|night|null",
          "startTime": "HH:MM or null",
          "endTime": "HH:MM or null",
          "notes": "any tips, costs, or details mentioned for this place",
          "cost": null,
          "currency": "EUR",
          "needsSplit": false,
          "splitActivities": []
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
- title and canonicalTitle MUST use the standard full official/common full name, never a local shorthand when the full name is knowable
- Keep originalMention as the original shorthand or wording from the source text when useful
- If the source uses a shorthand or alias such as '国博', '鸟巢', '水立方', '故宫', expand it to the canonical full place name in title/canonicalTitle
- If the source mentions a compound shorthand that refers to multiple places, such as '清北', '鸟巢水立方', or similar bundled mentions, set needsSplit=true and put multiple fully-formed activities into splitActivities; do not keep the bundled shorthand as a single final place
- When needsSplit=true, the parent activity is only a container hint and the backend will replace it with splitActivities, so splitActivities must be complete and usable on their own
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
    title: str,
    destination: str | None,
    region: str | None,
    country: str | None,
    country_code: str | None,
    category: str,
    language: str,
    semaphore: asyncio.Semaphore,
) -> TripLocationResponse | None:
    async with semaphore:
        from app.main import rank_and_convert

        normalized_category = category if category in ("attraction", "restaurant", "hotel", "transport", "shopping", "other") else "other"
        request = PlaceSearchRequest(
            query=search_name,
            category=normalized_category,
            scope="all",
            preferred_country_codes=[country_code] if country_code else [],
            country_filter_code=country_code or None,
            destination_context=build_destination_context(destination, region, country, country_code),
            language=language,
            limit=5,
        )

        for query in geocode_query_candidates(search_name, title, destination, country):
            request.query = query
            merged = []
            try:
                merged.extend(
                    await search_nominatim(
                        client,
                        query,
                        language,
                        request.country_filter_code,
                        request.limit,
                    )
                )
            except Exception as exc:
                logger.warning("geocode nominatim failed query=%s error=%r", query, exc)

            try:
                merged.extend(
                    await search_photon(
                        client,
                        query,
                        "en",
                        request.country_filter_code,
                        request.limit,
                    )
                )
            except Exception as exc:
                logger.warning("geocode photon failed query=%s error=%r", query, exc)

            ranked = rank_and_convert(merged, request)
            if not ranked:
                continue

            best = select_best_geocode_result(
                ranked,
                search_name=search_name,
                title=title,
                category=normalized_category,
            )
            if best is None:
                continue
            return TripLocationResponse(
                name=best.name,
                address=best.address or "",
                latitude=best.coordinate.latitude,
                longitude=best.coordinate.longitude,
                placeID=best.provider_place_id,
                country=best.country or "",
                countryCode=best.country_code or "",
                locality=best.locality or "",
                category=normalized_category,
            )

        return None


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
    resolved_region = ai_output.get("region", resolved_destination)
    resolved_country = ai_output.get("country", "")
    resolved_country_code = ai_output.get("countryCode", "")
    day_plans_raw = normalize_day_plans_raw(ai_output.get("dayPlans", []))
    warnings: list[str] = []

    # Collect all places that need geocoding
    geocode_tasks = []
    task_indices = []  # (day_idx, activity_idx)

    for day_idx, day in enumerate(day_plans_raw):
        for act_idx, act in enumerate(day.get("activities", [])):
            search_name = act.get("searchName") or act.get("title", "")
            category = act.get("category", "other")
            if search_name:
                geocode_tasks.append((search_name, standardized_activity_title(act), act.get("originalMention", ""), category))
                task_indices.append((day_idx, act_idx))

    # Geocode all places concurrently
    semaphore = asyncio.Semaphore(settings.ai_geocode_concurrency)
    headers = {"User-Agent": settings.user_agent}
    timeout = httpx.Timeout(settings.request_timeout_seconds)

    async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
        coros = [
            geocode_single_place(
                client,
                search_name=name,
                title=title or original_mention,
                destination=resolved_destination,
                region=resolved_region,
                country=resolved_country,
                country_code=resolved_country_code,
                category=cat,
                language=language,
                semaphore=semaphore,
            )
            for name, title, original_mention, cat in geocode_tasks
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

    prune_far_outlier_locations(
        day_plans_raw=day_plans_raw,
        location_map=location_map,
        resolved_destination=resolved_destination,
        resolved_region=resolved_region,
        resolved_country_code=resolved_country_code,
        warnings=warnings,
    )

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


def build_destination_context(
    destination: str | None,
    region: str | None,
    country: str | None,
    country_code: str | None,
) -> DestinationContext | None:
    normalized_country = (country or "").strip()
    normalized_country_code = (country_code or "").strip().upper()
    seeds: list[DestinationSeed] = []

    for name in [destination, region]:
        trimmed = (name or "").strip()
        if not trimmed:
            continue
        seed = DestinationSeed(
            name=trimmed,
            country=normalized_country or trimmed,
            country_code=normalized_country_code,
        )
        if seed not in seeds:
            seeds.append(seed)

    if not seeds:
        return None

    return DestinationContext(trip_id="ai-itinerary-import", destinations=seeds)


def normalize_day_plans_raw(day_plans_raw: list[dict]) -> list[dict]:
    normalized_days: list[dict] = []
    for day in day_plans_raw:
        normalized_day = dict(day)
        activities = day.get("activities", [])
        normalized_activities: list[dict] = []
        for activity in activities:
            normalized_activities.extend(normalize_activity_dict(activity))
        normalized_day["activities"] = normalized_activities
        normalized_days.append(normalized_day)
    return normalized_days


def normalize_activity_dict(activity: dict) -> list[dict]:
    if activity.get("needsSplit") and isinstance(activity.get("splitActivities"), list):
        split_activities: list[dict] = []
        for split_activity in activity.get("splitActivities", []):
            split_activities.extend(normalize_activity_dict(merged_activity_payload(activity, split_activity)))
        if split_activities:
            return split_activities

    title = standardized_activity_title(activity)
    rule = canonical_activity_rule(title)
    if rule is None:
        normalized = dict(activity)
        normalized["title"] = title or str(activity.get("title") or "").strip()
        if title and not normalized.get("canonicalTitle"):
            normalized["canonicalTitle"] = title
        return [normalized]

    if "split" in rule:
        split_activities: list[dict] = []
        for split_entry in rule["split"]:
            normalized = dict(activity)
            normalized["title"] = split_entry["title"]
            normalized["canonicalTitle"] = split_entry["title"]
            normalized["searchName"] = split_entry["searchName"]
            normalized["category"] = split_entry.get("category", activity.get("category", "other"))
            split_activities.append(normalized)
        return split_activities

    normalized = dict(activity)
    normalized["title"] = rule["title"]
    normalized["canonicalTitle"] = rule["title"]
    normalized["searchName"] = rule["search_name"]
    return [normalized]


def merged_activity_payload(parent: dict, child: dict) -> dict:
    merged = dict(parent)
    merged.update(child)
    merged.pop("splitActivities", None)
    merged["needsSplit"] = False
    if parent.get("originalMention") and not merged.get("originalMention"):
        merged["originalMention"] = parent.get("originalMention")
    return merged


def standardized_activity_title(activity: dict) -> str:
    for key in ("canonicalTitle", "title"):
        value = str(activity.get(key) or "").strip()
        if value:
            return value
    return ""


def canonical_activity_rule(title: str) -> dict | None:
    normalized_title = compact_match_key(title)
    if not normalized_title:
        return None

    for key, rule in CANONICAL_ACTIVITY_RULES.items():
        if compact_match_key(key) == normalized_title:
            return rule
    return None


def geocode_query_candidates(
    search_name: str,
    title: str,
    destination: str | None,
    country: str | None,
) -> list[str]:
    candidates: list[str] = []
    candidates.extend(rewritten_landmark_queries(search_name))
    candidates.extend(rewritten_landmark_queries(title))
    candidates.extend([search_name, title])

    simplified = simplify_activity_query(search_name) or simplify_activity_query(title)
    if simplified:
        if destination:
            candidates.append(f"{simplified} {destination}")
        else:
            candidates.append(simplified)
        if country and destination:
            candidates.append(f"{simplified} {destination} {country}")

    deduped: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        trimmed = " ".join((item or "").split())
        if not trimmed:
            continue
        normalized = trimmed.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(trimmed)
        if len(deduped) >= 4:
            break
    return deduped


def simplify_activity_query(value: str) -> str:
    if not value:
        return ""

    simplified = re.sub(r"\([^)]*\)", " ", value)
    simplified = re.sub(
        r"\b(river cruise|cruise|boat tour|boat trip|boat ride|sightseeing tour|walking tour|night tour|tour)\b",
        " ",
        simplified,
        flags=re.IGNORECASE,
    )
    simplified = re.sub(r"\s+", " ", simplified).strip(" ,-/")
    return simplified


def rewritten_landmark_queries(value: str) -> tuple[str, ...]:
    key = compact_match_key(value)
    if not key:
        return ()
    return LANDMARK_QUERY_REWRITES.get(key, ())


def select_best_geocode_result(ranked: list, search_name: str, title: str, category: str = "other"):
    strict_hints = landmark_name_hints(search_name, title)
    for item in ranked:
        if should_reject_result(item, category):
            continue
        if not strict_hints:
            return item
        if result_matches_hints(item, strict_hints):
            return item
    return None


def landmark_name_hints(search_name: str, title: str) -> tuple[str, ...]:
    hints: list[str] = []
    for value in [search_name, title]:
        for rewritten in rewritten_landmark_queries(value):
            hints.append(rewritten)
        hints.append(value)

    deduped: list[str] = []
    seen: set[str] = set()
    for hint in hints:
        normalized = compact_match_key(hint)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return tuple(deduped)


def result_matches_hints(item, hints: tuple[str, ...]) -> bool:
    candidate_texts = [
        compact_match_key(item.name),
        compact_match_key(item.address or ""),
        compact_match_key(item.subtitle or ""),
        compact_match_key(item.locality or ""),
    ]
    candidate_texts = [text for text in candidate_texts if text]
    if not candidate_texts:
        return False

    for hint in hints:
        for candidate in candidate_texts:
            if hint in candidate or candidate in hint:
                return True
    return False


def should_reject_result(item, category: str) -> bool:
    if category != "attraction":
        return False

    item_text = " ".join(
        filter(
            None,
            [
                normalize_text_for_match(getattr(item, "name", "") or ""),
                normalize_text_for_match(getattr(item, "address", "") or ""),
                normalize_text_for_match(getattr(item, "subtitle", "") or ""),
                normalize_text_for_match(getattr(item, "place_type", "") or ""),
            ],
        )
    )
    return any(token in item_text for token in ATTRACTION_RESULT_REJECT_TOKENS)


def compact_match_key(value: str | None) -> str:
    if not value:
        return ""
    normalized = normalize_text_for_match(str(value))
    normalized = normalized.replace("'", " ")
    normalized = re.sub(r"[^\w\u4e00-\u9fff]+", " ", normalized)
    return "".join(normalized.split())


def prune_far_outlier_locations(
    day_plans_raw: list[dict],
    location_map: dict[tuple[int, int], TripLocationResponse | None],
    resolved_destination: str,
    resolved_region: str,
    resolved_country_code: str,
    warnings: list[str],
) -> None:
    trip_reference = trip_reference_location(list(location_map.values()))

    for day_idx, day in enumerate(day_plans_raw):
        day_entries: list[tuple[int, TripLocationResponse]] = []
        for act_idx, _ in enumerate(day.get("activities", [])):
            location = location_map.get((day_idx, act_idx))
            if is_usable_location(location):
                day_entries.append((act_idx, location))

        if len(day_entries) < 2:
            continue

        day_reference = dominant_day_reference(
            [location for _, location in day_entries],
            resolved_destination=resolved_destination,
            resolved_region=resolved_region,
            resolved_country_code=resolved_country_code,
        )
        if day_reference is None:
            continue

        for act_idx, location in day_entries:
            day_distance_km = haversine_km(
                location.latitude,
                location.longitude,
                day_reference.latitude,
                day_reference.longitude,
            )
            trip_distance_km = None
            if trip_reference is not None:
                trip_distance_km = haversine_km(
                    location.latitude,
                    location.longitude,
                    trip_reference.latitude,
                    trip_reference.longitude,
                )

            same_day_locality = same_locality(location.locality, day_reference.locality)
            same_trip_country = not resolved_country_code or location.countryCode.upper() == resolved_country_code.upper()
            is_far_from_day_cluster = day_distance_km > MAX_DAY_OUTLIER_DISTANCE_KM and not same_day_locality
            is_far_from_trip_cluster = trip_distance_km is not None and trip_distance_km > MAX_TRIP_OUTLIER_DISTANCE_KM

            if not same_trip_country or is_far_from_day_cluster or is_far_from_trip_cluster:
                title = day.get("activities", [])[act_idx].get("title", "") or location.name
                warnings.append(
                    f"Removed outlier location: {title} ({location.locality or location.country or 'unknown'})"
                )
                location_map[(day_idx, act_idx)] = None


def dominant_day_reference(
    locations: list[TripLocationResponse],
    resolved_destination: str,
    resolved_region: str,
    resolved_country_code: str,
) -> TripLocationResponse | None:
    if not locations:
        return None

    locality_votes: dict[str, int] = {}
    for location in locations:
        locality = normalized_locality_key(location.locality)
        if locality:
            locality_votes[locality] = locality_votes.get(locality, 0) + 1

    preferred_keys = {
        normalized_locality_key(resolved_destination),
        normalized_locality_key(resolved_region),
    } - {""}

    dominant_key = ""
    if locality_votes:
        dominant_key = max(
            locality_votes.items(),
            key=lambda item: (
                item[1],
                1 if item[0] in preferred_keys else 0,
                item[0],
            ),
        )[0]

    cluster = [
        location for location in locations
        if not dominant_key or normalized_locality_key(location.locality) == dominant_key
    ]
    if not cluster:
        cluster = locations

    if resolved_country_code:
        same_country_cluster = [
            location for location in cluster
            if location.countryCode.upper() == resolved_country_code.upper()
        ]
        if same_country_cluster:
            cluster = same_country_cluster

    return centroid_location(cluster)


def trip_reference_location(locations: list[TripLocationResponse | None]) -> TripLocationResponse | None:
    usable = [location for location in locations if is_usable_location(location)]
    if len(usable) < 2:
        return None
    return centroid_location(usable)


def centroid_location(locations: list[TripLocationResponse]) -> TripLocationResponse | None:
    if not locations:
        return None

    latitude = sum(location.latitude for location in locations) / len(locations)
    longitude = sum(location.longitude for location in locations) / len(locations)
    locality = most_common_value({
        key: sum(1 for location in locations if normalized_locality_key(location.locality) == key)
        for key in {normalized_locality_key(location.locality) for location in locations if normalized_locality_key(location.locality)}
    }) or ""
    country = most_common_value({
        location.country: sum(1 for item in locations if item.country == location.country)
        for location in locations if location.country
    }) or ""
    country_code = most_common_value({
        location.countryCode: sum(1 for item in locations if item.countryCode == location.countryCode)
        for location in locations if location.countryCode
    }) or ""

    return TripLocationResponse(
        name="cluster-center",
        address="",
        latitude=latitude,
        longitude=longitude,
        placeID=None,
        country=country,
        countryCode=country_code,
        locality=locality,
        category="other",
    )


def is_usable_location(location: TripLocationResponse | None) -> bool:
    return bool(location) and (location.latitude != 0.0 or location.longitude != 0.0)


def same_locality(left: str, right: str) -> bool:
    left_key = normalized_locality_key(left)
    right_key = normalized_locality_key(right)
    return bool(left_key) and left_key == right_key


def normalized_locality_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", "", normalize_text_for_match(str(value)))


def normalize_text_for_match(value: str) -> str:
    return str(value or "").strip().lower()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


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
