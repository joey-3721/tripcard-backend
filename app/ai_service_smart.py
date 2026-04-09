from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time

import httpx

from app.ai_service import (
    SYSTEM_PROMPT,
    call_ai_json,
    call_ai_model,
    get_ai_token,
    infer_country_code,
    normalize_ai_provider,
    normalize_country_code,
    parse_itinerary_from_ai_output,
)
from app.ai_service_no_geocoding import build_parse_itinerary_no_geocoding_response
from app.cache_mysql import MySQLCache
from app.config import settings
from app.schemas import (
    ActivityResponseSmart,
    DayPlanResponseSmart,
    ParseItinerarySmartResponse,
)

logger = logging.getLogger("tripcard-backend")

SEGMENTATION_PROMPT = """\
You are a travel itinerary pre-parser.

Return ONLY valid JSON, no markdown fences, no explanation. The JSON schema:

{
  "destination": "string",
  "country": "string",
  "countryCode": "string",
  "region": "string",
  "totalDays": number,
  "segments": [
    {
      "segmentNumber": 1,
      "startDay": 1,
      "endDay": 2,
      "rawText": "string"
    }
  ]
}

Rules:
- Your job is ONLY to identify destination context and split the raw itinerary into smaller sequential segments for downstream parsing.
- Prefer 2 travel days per segment.
- Never put more than 2 travel days in one segment.
- If a segment is dense, ambiguous, or long, use 1 day instead.
- rawText should preserve the original wording and order as much as possible and only contain the relevant part for that segment.
- Ignore noisy page markers like p1-3 when possible, but keep enough surrounding text to preserve meaning.
- If explicit day markers are missing, infer boundaries from chronology, hotel check-in/check-out, major transfers, and temporal phrases such as "第二天", "上午", "晚上", "次日".
- segments must stay in source order and should cover the whole itinerary with no intentional omissions.
- Keep segments small and safe. If uncertain, prefer more segments, not fewer.
"""

SEGMENTATION_TEXT_LENGTH_THRESHOLD = 260
SEGMENTATION_LINE_THRESHOLD = 5
SEGMENTATION_EXPLICIT_DAY_THRESHOLD = 3
SEGMENT_PARSE_CONCURRENCY = 6
SEGMENT_PARSE_RETRIES = 2
SEGMENT_MAX_EXPECTED_DAYS = 2
SMART_PARSE_CACHE_VERSION = 1


async def parse_itinerary_smart(
    text: str,
    destination: str | None,
    language: str,
    model_name: str = "deepseek",
    progress_callback=None,
) -> ParseItinerarySmartResponse:
    started_at = time.perf_counter()
    provider = normalize_ai_provider(model_name)
    cache_key = build_smart_parse_cache_key(
        text=text,
        destination=destination,
        language=language,
        provider=provider,
    )
    db = MySQLCache()
    cached_response = db.get_ai_cache(cache_key)
    if cached_response is not None:
        response = ParseItinerarySmartResponse.model_validate(cached_response)
        logger.info(
            "parse-itinerary-smart cache hit provider=%s destination=%s language=%s elapsed=%.3fs",
            provider,
            destination or "",
            language,
            time.perf_counter() - started_at,
        )
        return response

    logger.info(
        "parse-itinerary-smart cache miss provider=%s destination=%s language=%s",
        provider,
        destination or "",
        language,
    )
    await emit_progress(progress_callback, 5, "开始 AI 解析")
    token_info = get_ai_token(provider)
    ai_output, preparse_warnings, segmented = await resolve_ai_output_smart(
        text=text,
        destination=destination,
        provider=provider,
        token_info=token_info,
        progress_callback=progress_callback,
    )
    ai_elapsed = time.perf_counter() - started_at
    await emit_progress(progress_callback, 55, "AI 解析完成，开始地点识别")

    resolved_destination = ai_output.get("destination", destination or "")
    resolved_region = ai_output.get("region", resolved_destination)
    resolved_country = ai_output.get("country", "")
    resolved_country_code = normalize_country_code(ai_output.get("countryCode", ""))

    if not resolved_country_code:
        infer_started_at = time.perf_counter()
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
        logger.info(
            "parse-itinerary-smart inferred country_code=%s provider=%s elapsed=%.3fs",
            resolved_country_code,
            provider,
            time.perf_counter() - infer_started_at,
        )
        if resolved_country_code:
            ai_output["countryCode"] = resolved_country_code

    if resolved_country_code.upper() == "CN":
        response = build_parse_itinerary_no_geocoding_response(ai_output=ai_output, destination=destination)
        warnings = dedupe_warnings(preparse_warnings + response.warnings)
        total_elapsed = time.perf_counter() - started_at
        smart_response = ParseItinerarySmartResponse(
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
                            timeBucket=None,
                            startTime=None,
                            endTime=None,
                            notes="",
                        )
                        for activity in day.activities
                    ],
                    notes="",
                )
                for day in response.dayPlans
            ],
            warnings=warnings,
            geocodingMode="client_apple",
        )
        db.set_ai_cache(
            cache_key,
            json.loads(smart_response.model_dump_json()),
        )
        logger.info(
            "parse-itinerary-smart response provider=%s segmented=%s geocoding_mode=%s ai_elapsed=%.3fs total_elapsed=%.3fs payload=%s",
            provider,
            segmented,
            "client_apple",
            ai_elapsed,
            total_elapsed,
            json.dumps(smart_response.model_dump(mode="json"), ensure_ascii=False),
        )
        return smart_response

    no_geocoding_response = build_parse_itinerary_no_geocoding_response(ai_output=ai_output, destination=destination)
    response = await parse_itinerary_from_ai_output(
        ai_output=ai_output,
        destination=destination,
        language=language,
        preserve_unresolved=True,
        progress_callback=progress_callback,
        progress_range=(55, 98),
    )
    should_filter_empty_locations = resolved_country_code.upper() != "CN"
    filtered_day_plans: list[DayPlanResponseSmart] = []
    dropped_activity_count = 0
    dropped_day_count = 0
    for day_index, day in enumerate(response.dayPlans):
        filtered_activities: list[ActivityResponseSmart] = []
        paired_no_geo_activities = no_geocoding_response.dayPlans[day_index].activities if day_index < len(no_geocoding_response.dayPlans) else []
        for activity_index, activity in enumerate(day.activities):
            if should_filter_empty_locations and activity.location is None:
                dropped_activity_count += 1
                continue
            search_name = paired_no_geo_activities[activity_index].searchName if activity_index < len(paired_no_geo_activities) else None
            filtered_activities.append(
                ActivityResponseSmart(
                    id=activity.id,
                    title=activity.title,
                    searchName=search_name,
                    category=activity.category,
                    location=activity.location,
                    timeBucket=None,
                    startTime=None,
                    endTime=None,
                    notes="",
                )
            )
        if should_filter_empty_locations and not filtered_activities:
            dropped_day_count += 1
            continue
        filtered_day_plans.append(
            DayPlanResponseSmart(
                id=day.id,
                dayNumber=day.dayNumber,
                date=day.date,
                activities=filtered_activities,
                notes="",
            )
        )
    warnings = dedupe_warnings(preparse_warnings + response.warnings)
    if should_filter_empty_locations and dropped_activity_count:
        warnings.append(f"Filtered {dropped_activity_count} activity items without coordinates before returning to client.")
    if should_filter_empty_locations and dropped_day_count:
        warnings.append(f"Filtered {dropped_day_count} empty day plans after coordinate cleanup.")
    warnings = dedupe_warnings(warnings)
    if should_filter_empty_locations and not filtered_day_plans and no_geocoding_response.dayPlans:
        logger.warning(
            "parse-itinerary-smart no geocoded activities survived provider=%s destination=%s total_days=%s dropped_activities=%s dropped_days=%s",
            provider,
            resolved_destination,
            response.totalDays,
            dropped_activity_count,
            dropped_day_count,
        )
        raise RuntimeError("未识别到可用地点，请稍后重试、缩短文案，或切换模型。")
    total_elapsed = time.perf_counter() - started_at
    smart_response = ParseItinerarySmartResponse(
        destination=response.destination,
        totalDays=response.totalDays,
        summary=response.summary,
        dayPlans=filtered_day_plans,
        warnings=warnings,
        geocodingMode="backend_geoapify",
    )
    db.set_ai_cache(
        cache_key,
        json.loads(smart_response.model_dump_json()),
    )
    await emit_progress(progress_callback, 100, "解析完成")
    logger.info(
        "parse-itinerary-smart response provider=%s segmented=%s geocoding_mode=%s ai_elapsed=%.3fs total_elapsed=%.3fs dropped_activities=%s dropped_days=%s payload=%s",
        provider,
        segmented,
        "backend_geoapify",
        ai_elapsed,
        total_elapsed,
        dropped_activity_count,
        dropped_day_count,
        json.dumps(smart_response.model_dump(mode="json"), ensure_ascii=False),
    )
    return smart_response


def build_smart_parse_cache_key(
    *,
    text: str,
    destination: str | None,
    language: str,
    provider: str,
) -> str:
    payload = json.dumps(
        {
            "cache_version": SMART_PARSE_CACHE_VERSION,
            "text": text,
            "destination": destination,
            "language": language,
            "provider": provider,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def resolve_ai_output_smart(
    text: str,
    destination: str | None,
    provider: str,
    token_info: dict,
    progress_callback=None,
) -> tuple[dict, list[str], bool]:
    if not should_use_segmented_parsing(text, provider):
        logger.info(
            "parse-itinerary-smart using single-shot parse provider=%s text_length=%s",
            provider,
            len(text),
        )
        await emit_progress(progress_callback, 15, "单次 AI 解析中")
        return await call_ai_model(text, destination, token_info, provider), [], False

    segmentation_started_at = time.perf_counter()
    try:
        segmentation_output = await call_segmentation_model(
            text=text,
            destination=destination,
            provider=provider,
            token_info=token_info,
        )
        segments, segmentation_warnings = normalize_segments(segmentation_output, text)
    except Exception as exc:
        logger.warning("parse-itinerary-smart segmentation failed provider=%s error=%r", provider, exc)
        ai_output = await call_ai_model(text, destination, token_info, provider)
        return ai_output, ["Segment pre-parse failed; used single-shot parsing."], False

    logger.info(
        "parse-itinerary-smart segmentation provider=%s elapsed=%.3fs segments=%s estimated_total_days=%s",
        provider,
        time.perf_counter() - segmentation_started_at,
        len(segments),
        segmentation_output.get("totalDays", 0),
    )
    await emit_progress(progress_callback, 15, f"已拆分为 {len(segments)} 段")
    if len(segments) <= 1:
        ai_output = await call_ai_model(text, destination, token_info, provider)
        warnings = segmentation_warnings
        if segments:
            warnings.append("Segmentation returned only one chunk; used single-shot parsing.")
        return ai_output, dedupe_warnings(warnings), False

    parsed_segments, segment_warnings = await parse_segments_concurrently(
        segments=segments,
        destination=destination,
        provider=provider,
        token_info=token_info,
        progress_callback=progress_callback,
    )
    if not parsed_segments:
        logger.warning("parse-itinerary-smart all segments failed; falling back to single-shot parsing provider=%s", provider)
        ai_output = await call_ai_model(text, destination, token_info, provider)
        return ai_output, dedupe_warnings(segmentation_warnings + segment_warnings + ["All segments failed; used single-shot parsing."]), False

    merged_output = merge_segment_outputs(
        segmentation_output=segmentation_output,
        parsed_segments=parsed_segments,
        fallback_destination=destination,
    )
    return merged_output, dedupe_warnings(segmentation_warnings + segment_warnings), True


async def call_segmentation_model(
    text: str,
    destination: str | None,
    provider: str,
    token_info: dict,
) -> dict:
    user_content = ""
    if destination:
        user_content += f"Destination context: {destination}\n\n"
    user_content += f"Split this itinerary into safe parsing segments:\n\n{text}"
    return await call_ai_json(
        token_info=token_info,
        provider=provider,
        system_prompt=SEGMENTATION_PROMPT,
        user_content=user_content,
        temperature=0.1,
        max_tokens=1400,
        request_label="segment_itinerary",
        destination=destination,
        text_length=len(text),
    )


def normalize_segments(segmentation_output: dict, source_text: str) -> tuple[list[dict], list[str]]:
    raw_segments = segmentation_output.get("segments", [])
    if not isinstance(raw_segments, list):
        return [], ["Segment pre-parse returned invalid segments payload."]

    normalized_segments: list[dict] = []
    warnings: list[str] = []
    for index, raw_segment in enumerate(raw_segments, start=1):
        if not isinstance(raw_segment, dict):
            warnings.append(f"Skipped invalid segment payload at index {index}.")
            continue

        raw_text = normalize_segment_text(raw_segment.get("rawText"))
        if not raw_text:
            warnings.append(f"Skipped empty segment {index}.")
            continue

        start_day = safe_int(raw_segment.get("startDay")) or index
        end_day = safe_int(raw_segment.get("endDay")) or start_day
        expected_days = max(1, min(SEGMENT_MAX_EXPECTED_DAYS, end_day - start_day + 1))

        if raw_text not in source_text and len(raw_text) < 20:
            warnings.append(f"Skipped too-short segment {index}.")
            continue

        normalized_segments.append(
            {
                "segmentNumber": safe_int(raw_segment.get("segmentNumber")) or index,
                "expectedDays": expected_days,
                "rawText": raw_text,
            }
        )

    normalized_segments.sort(key=lambda item: item["segmentNumber"])

    deduped_segments: list[dict] = []
    seen_texts: set[str] = set()
    for segment in normalized_segments:
        key = re.sub(r"\s+", "", segment["rawText"]).lower()
        if not key or key in seen_texts:
            warnings.append(f"Skipped duplicate segment {segment['segmentNumber']}.")
            continue
        seen_texts.add(key)
        deduped_segments.append(segment)

    return deduped_segments, warnings


def normalize_segment_text(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines()]
    cleaned = "\n".join(line for line in lines if line)
    return re.sub(r"[ \t]+", " ", cleaned).strip()


async def parse_segments_concurrently(
    segments: list[dict],
    destination: str | None,
    provider: str,
    token_info: dict,
    progress_callback=None,
) -> tuple[list[dict], list[str]]:
    semaphore = asyncio.Semaphore(SEGMENT_PARSE_CONCURRENCY)
    warnings: list[str] = []
    completed_segments = 0

    async def run_segment(segment: dict) -> tuple[dict | None, list[str]]:
        async with semaphore:
            return await parse_segment_with_retries(
                segment=segment,
                destination=destination,
                provider=provider,
                token_info=token_info,
            )

    parsed_segments: list[dict] = []
    if not segments:
        return parsed_segments, warnings

    tasks = [asyncio.create_task(run_segment(segment)) for segment in segments]
    for finished_task in asyncio.as_completed(tasks):
        result, segment_warnings = await finished_task
        completed_segments += 1
        warnings.extend(segment_warnings)
        if result is not None:
            parsed_segments.append(result)
        progress = 15 + int((completed_segments / len(segments)) * 35)
        await emit_progress(progress_callback, progress, f"AI 分段解析 {completed_segments}/{len(segments)}")

    parsed_segments.sort(key=lambda item: item["segmentNumber"])
    return parsed_segments, warnings


async def parse_segment_with_retries(
    segment: dict,
    destination: str | None,
    provider: str,
    token_info: dict,
) -> tuple[dict | None, list[str]]:
    warnings: list[str] = []
    for attempt in range(1, SEGMENT_PARSE_RETRIES + 1):
        started_at = time.perf_counter()
        try:
            ai_output = await call_ai_json(
                token_info=token_info,
                provider=provider,
                system_prompt=SYSTEM_PROMPT,
                user_content=build_segment_user_content(segment["rawText"], destination, segment["expectedDays"], attempt),
                temperature=0.1,
                max_tokens=1800,
                request_label=f"parse_segment_{segment['segmentNumber']}_try_{attempt}",
                destination=destination,
                text_length=len(segment["rawText"]),
            )
            day_plans = ai_output.get("dayPlans", [])
            day_count = len(day_plans) if isinstance(day_plans, list) else 0
            if day_count <= 0:
                raise ValueError("segment parser returned no dayPlans")

            logger.info(
                "parse-itinerary-smart segment success provider=%s segment=%s expected_days=%s parsed_days=%s elapsed=%.3fs attempt=%s",
                provider,
                segment["segmentNumber"],
                segment["expectedDays"],
                day_count,
                time.perf_counter() - started_at,
                attempt,
            )
            return {
                "segmentNumber": segment["segmentNumber"],
                "expectedDays": segment["expectedDays"],
                "aiOutput": ai_output,
            }, warnings
        except Exception as exc:
            logger.warning(
                "parse-itinerary-smart segment failed provider=%s segment=%s attempt=%s elapsed=%.3fs error=%r",
                provider,
                segment["segmentNumber"],
                attempt,
                time.perf_counter() - started_at,
                exc,
            )
            if attempt == SEGMENT_PARSE_RETRIES:
                warnings.append(f"Segment {segment['segmentNumber']} failed after retry and was skipped.")

    return None, warnings


def build_segment_user_content(text: str, destination: str | None, expected_days: int, attempt: int) -> str:
    lines = [
        f"This excerpt should contain about {expected_days} travel day(s).",
        "Preserve the full schema exactly.",
        "Do not collapse the result into description/activity shortcuts.",
    ]
    if attempt > 1:
        lines.append("Retry carefully: keep dayNumber, activities, title, originalMention, canonicalTitle, searchName, activityType, locationMode, category.")
    lines.append("Keep timeBucket/startTime/endTime null unless an exact time is explicitly stated.")
    lines.append("Keep activity notes and day notes empty unless absolutely required for place disambiguation.")

    user_content = ""
    if destination:
        user_content += f"Destination context: {destination}\n\n"
    user_content += "\n".join(lines)
    user_content += f"\n\nParse this itinerary:\n\n{text}"
    return user_content


def merge_segment_outputs(
    segmentation_output: dict,
    parsed_segments: list[dict],
    fallback_destination: str | None,
) -> dict:
    merged_day_plans: list[dict] = []
    next_day_number = 1
    for segment in parsed_segments:
        day_plans = segment["aiOutput"].get("dayPlans", [])
        if not isinstance(day_plans, list):
            continue
        for day in day_plans:
            if not isinstance(day, dict):
                continue
            merged_day = dict(day)
            merged_day["dayNumber"] = next_day_number
            merged_day_plans.append(merged_day)
            next_day_number += 1

    destination = first_non_empty(
        segmentation_output.get("destination"),
        *(segment["aiOutput"].get("destination") for segment in parsed_segments),
        fallback_destination,
        "",
    )
    country = first_non_empty(
        segmentation_output.get("country"),
        *(segment["aiOutput"].get("country") for segment in parsed_segments),
        "",
    )
    country_code = normalize_country_code(
        first_non_empty(
            segmentation_output.get("countryCode"),
            *(segment["aiOutput"].get("countryCode") for segment in parsed_segments),
            "",
        )
    )
    candidate_regions = [
        str(value or "").strip()
        for value in [segmentation_output.get("region"), *(segment["aiOutput"].get("region") for segment in parsed_segments)]
        if str(value or "").strip()
    ]
    normalized_regions = {region for region in candidate_regions if region and region != destination}
    if len(normalized_regions) == 1:
        region = next(iter(normalized_regions))
    elif not normalized_regions:
        region = destination
    else:
        region = destination

    return {
        "title": "",
        "destination": destination,
        "country": country,
        "countryCode": country_code,
        "region": region,
        "totalDays": len(merged_day_plans),
        "dayPlans": merged_day_plans,
    }


def should_use_segmented_parsing(text: str, provider: str) -> bool:
    compact_text = str(text or "").strip()
    if not compact_text:
        return False

    non_empty_lines = [line for line in compact_text.splitlines() if line.strip()]
    explicit_day_matches = len(re.findall(r"(?:^|\s)(?:d|day)\s*\d+", compact_text, flags=re.IGNORECASE))
    if provider == "qwen-turbo":
        return (
            len(compact_text) >= SEGMENTATION_TEXT_LENGTH_THRESHOLD
            or len(non_empty_lines) >= SEGMENTATION_LINE_THRESHOLD
            or explicit_day_matches >= SEGMENTATION_EXPLICIT_DAY_THRESHOLD
        )

    return (
        len(compact_text) >= SEGMENTATION_TEXT_LENGTH_THRESHOLD * 2
        or len(non_empty_lines) >= SEGMENTATION_LINE_THRESHOLD + 2
        or explicit_day_matches >= SEGMENTATION_EXPLICIT_DAY_THRESHOLD + 2
    )


def safe_int(value: object) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def first_non_empty(*values: object) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def dedupe_warnings(warnings: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for warning in warnings:
        normalized = str(warning or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


async def emit_progress(progress_callback, progress: int, message: str) -> None:
    if progress_callback is None:
        return
    await progress_callback(max(0, min(100, int(progress))), message)
