from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.cache_mysql import MySQLCache
from app.config import settings
from app.providers.base import ProviderPlace
from app.providers.gaode import search_gaode
from app.providers.geoapify import fetch_geoapify_batch, search_geoapify
from app.schemas import (
    ParseItineraryRequest,
    ParseItineraryResponse,
    ParseItineraryResponseNoLocation,
    ParseItinerarySmartResponse,
    PlaceResult,
    PlaceSearchMeta,
    PlaceSearchRequest,
    PlaceSearchResponse,
)

logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
logger = logging.getLogger("tripcard-backend")
cache = MySQLCache() if settings.cache_enabled else None
db = MySQLCache()  # for ai_tokens table operations


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("starting backend")
    if cache is not None:
        cache.ensure_table()
        cache.cleanup()
    db.ensure_ai_tokens_table()
    db.ensure_ai_parse_cache_table()
    db.ensure_place_geocode_cache_table()
    yield


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/place-search", response_model=PlaceSearchResponse)
async def place_search(request: PlaceSearchRequest) -> PlaceSearchResponse:
    trace_id = str(uuid.uuid4())
    query = request.query.strip()
    if not query:
        return PlaceSearchResponse(
            query=request.query,
            trace_id=trace_id,
            results=[],
            meta=PlaceSearchMeta(
                scope=request.scope,
                country_filter_code=request.country_filter_code,
                preferred_country_codes=request.preferred_country_codes,
                providers_used=[],
                cache_hit=False,
                self_hosted_data=False,
            ),
        )

    cache_key = build_cache_key(request)
    if cache is not None:
        cached = cache.get(cache_key)
        if cached is not None:
            cached["trace_id"] = trace_id
            cached["meta"]["cache_hit"] = True
            return PlaceSearchResponse.model_validate(cached)

    context_queries = build_queries(query, request)
    providers_used: list[str] = []
    merged: list[ProviderPlace] = []

    headers = {"User-Agent": settings.user_agent}
    timeout = httpx.Timeout(settings.request_timeout_seconds)
    async with httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=True) as client:
        use_china_provider = should_use_china_provider(request)
        if use_china_provider:
            for candidate in context_queries:
                try:
                    merged.extend(
                        await search_gaode(
                            client,
                            candidate,
                            request.language,
                            request.country_filter_code,
                            request.limit,
                        )
                    )
                    if "gaode" not in providers_used:
                        providers_used.append("gaode")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("gaode failed query=%s error=%r", candidate, exc)
        else:
            geoapify_batches = await fetch_geoapify_place_search_batches(client, context_queries, request)
            for candidate in context_queries:
                merged.extend(geoapify_batches.get(candidate, []))
            if any(geoapify_batches.values()):
                providers_used.append("geoapify")


    results = rank_and_convert(merged, request)
    response = PlaceSearchResponse(
        query=request.query,
        trace_id=trace_id,
        results=results[: request.limit],
        meta=PlaceSearchMeta(
            scope=request.scope,
            country_filter_code=request.country_filter_code,
            preferred_country_codes=request.preferred_country_codes,
            fallback_used=False,
            providers_used=providers_used,
            cache_hit=False,
            self_hosted_data=False,
        ),
    )
    if cache is not None and response.results:
        cache.set(
            cache_key,
            json.loads(response.model_dump_json()),
            settings.cache_ttl_seconds,
        )
    return response


async def fetch_geoapify_place_search_batches(
    client: httpx.AsyncClient,
    context_queries: list[str],
    request: PlaceSearchRequest,
) -> dict[str, list[ProviderPlace]]:
    try:
        return await fetch_geoapify_batch(
            client=client,
            queries=context_queries,
            language=request.language,
            country_filter_code=request.country_filter_code,
            limit=request.limit,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("geoapify batch failed error=%r", exc)
        batches: dict[str, list[ProviderPlace]] = {}
        for query in context_queries:
            try:
                batches[query] = await search_geoapify(
                    client,
                    query,
                    request.language,
                    request.country_filter_code,
                    request.limit,
                )
            except Exception as inner_exc:  # noqa: BLE001
                logger.warning("geoapify failed query=%s error=%r", query, inner_exc)
                if isinstance(inner_exc, httpx.HTTPStatusError):
                    logger.warning("geoapify response body=%s", inner_exc.response.text[:300])
                batches[query] = []
        return batches


def build_queries(query: str, request: PlaceSearchRequest) -> list[str]:
    queries = [query]
    destinations = request.destination_context.destinations if request.destination_context else []
    context_parts: list[str] = []
    for item in destinations[:4]:
        if item.name.strip():
            context_parts.append(item.name.strip())
        if item.country.strip():
            context_parts.append(item.country.strip())

    if context_parts:
        context = " ".join(dict.fromkeys(context_parts))
        queries.append(f"{query} {context}")
        queries.append(f"{context} {query}")

    deduped: list[str] = []
    seen: set[str] = set()
    for item in queries:
        normalized = " ".join(item.lower().split())
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(item)
    return deduped


def should_use_china_provider(request: PlaceSearchRequest) -> bool:
    if request.country_filter_code and request.country_filter_code.upper() == "CN":
        return True
    if len(request.preferred_country_codes) == 1 and request.preferred_country_codes[0].upper() == "CN":
        return True
    return False


def build_cache_key(request: PlaceSearchRequest) -> str:
    payload = json.dumps({"cache_version": 3, **request.model_dump(mode="json")}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def rank_and_convert(items: list[ProviderPlace], request: PlaceSearchRequest) -> list[PlaceResult]:
    preferred_codes = {code.upper() for code in request.preferred_country_codes if code}
    seen: set[str] = set()
    results: list[PlaceResult] = []
    normalized_query = normalize(request.query)
    query_forms = normalized_forms(request.query)
    strong_preferred_match_exists = has_strong_preferred_match(items, request, query_forms, preferred_codes)
    strong_nominatim_match_exists = has_strong_nominatim_match(items, request, query_forms, preferred_codes)

    scored = sorted(
        items,
        key=lambda item: ranking_tuple(
            item,
            request,
            normalized_query,
            query_forms,
            preferred_codes,
            request.country_filter_code,
            strong_preferred_match_exists,
            strong_nominatim_match_exists,
        ),
    )

    for item in scored:
        item_query_rank = query_match_rank(
            item,
            request,
            normalized_query,
            query_forms,
            normalized_forms(item.name, item.address, item.subtitle, item.locality),
        )
        if (
            strong_preferred_match_exists
            and preferred_codes
            and item.country_code.upper() not in preferred_codes
            and item_query_rank >= 4
        ):
            continue

        dedupe_key = "|".join(
            [
                normalize(item.name),
                item.country_code.upper(),
                f"{item.latitude:.5f}",
                f"{item.longitude:.5f}",
            ]
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        results.append(
            PlaceResult(
                id=f"{item.provider}|{item.provider_place_id or dedupe_key}",
                name=item.name,
                subtitle=item.subtitle,
                address=item.address,
                coordinate={"latitude": item.latitude, "longitude": item.longitude},
                country=item.country,
                country_code=item.country_code,
                locality=item.locality,
                place_type=item.place_type,
                category=request.category,
                provider=item.provider,
                provider_place_id=item.provider_place_id,
                score=score_value(item, request, normalized_query, query_forms, preferred_codes),
                matched_by=matched_by(item, request, normalized_query, query_forms, preferred_codes),
            )
        )
    return results


def ranking_tuple(
    item: ProviderPlace,
    request: PlaceSearchRequest,
    normalized_query: str,
    query_forms: set[str],
    preferred_codes: set[str],
    country_filter_code: str | None,
    strong_preferred_match_exists: bool,
    strong_nominatim_match_exists: bool,
) -> tuple[int, int, int, int, int, int, str]:
    code = item.country_code.upper()
    normalized_name = normalize(item.name)
    normalized_address = normalize(item.address or "")
    item_forms = normalized_forms(item.name, item.address, item.subtitle, item.locality)
    query_rank = query_match_rank(item, request, normalized_query, query_forms, item_forms)

    if country_filter_code and code == country_filter_code.upper():
        filter_rank = 0
    elif country_filter_code:
        filter_rank = 1
    else:
        filter_rank = 0

    preferred_rank = 0 if preferred_codes and code in preferred_codes else 1
    address_rank = 0 if item.address else 1
    locality_rank = locality_context_rank(item, request)

    provider_rank = 0
    if strong_nominatim_match_exists and item.provider != "nominatim" and query_rank <= 2:
        provider_rank = 1

    country_focus_rank = 0
    if strong_preferred_match_exists and preferred_codes and code not in preferred_codes:
        country_focus_rank = 1

    return (
        filter_rank,
        query_rank,
        country_focus_rank,
        preferred_rank,
        provider_rank,
        locality_rank + address_rank,
        normalized_name or normalized_address,
    )


def score_value(
    item: ProviderPlace,
    request: PlaceSearchRequest,
    normalized_query: str,
    query_forms: set[str],
    preferred_codes: set[str],
) -> float:
    score = 0.5
    item_forms = normalized_forms(item.name, item.address, item.subtitle, item.locality)
    query_rank = query_match_rank(item, request, normalized_query, query_forms, item_forms)
    if query_rank == 0:
        score += 0.35
    elif query_rank == 1:
        score += 0.25
    elif query_rank == 2:
        score += 0.15
    elif query_rank == 3:
        score += 0.08
    if preferred_codes and item.country_code.upper() in preferred_codes:
        score += 0.12
    if locality_context_rank(item, request) == 0:
        score += 0.06
    if item.address:
        score += 0.03
    return min(score, 0.99)


def matched_by(
    item: ProviderPlace,
    request: PlaceSearchRequest,
    normalized_query: str,
    query_forms: set[str],
    preferred_codes: set[str],
) -> list[str]:
    marks: list[str] = []
    item_forms = normalized_forms(item.name, item.address, item.subtitle, item.locality)
    query_rank = query_match_rank(item, request, normalized_query, query_forms, item_forms)
    if query_rank == 0:
        marks.append("name_exact")
    elif query_rank == 1:
        marks.append("name_prefix")
    elif query_rank == 2:
        marks.append("name_contains")
    elif query_rank == 3:
        marks.append("address_contains")
    if preferred_codes and item.country_code.upper() in preferred_codes:
        marks.append("preferred_country_boost")
    if locality_context_rank(item, request) == 0:
        marks.append("destination_context_match")
    if item.address:
        marks.append("has_address")
    return marks


def normalize(text: str) -> str:
    folded = unicodedata.normalize("NFKC", text).lower()
    folded = re.sub(r"[^\w\u4e00-\u9fff]+", " ", folded)
    return " ".join(folded.strip().split())


def normalized_forms(*values: str | None) -> set[str]:
    forms: set[str] = set()
    for value in values:
        if not value:
            continue
        normalized = normalize(value)
        if not normalized:
            continue
        forms.add(normalized)
        compact = compact_text(normalized)
        if compact:
            forms.add(compact)
    return {form for form in forms if form}


def query_match_rank(
    item: ProviderPlace,
    request: PlaceSearchRequest,
    normalized_query: str,
    query_forms: set[str],
    item_forms: set[str],
) -> int:
    if not item_forms:
        return 5

    compact_query = compact_text(normalized_query)
    compact_item_forms = {compact_text(form) for form in item_forms if compact_text(form)}
    if compact_query in compact_item_forms:
        return 0

    for form in compact_item_forms:
        if form.startswith(compact_query):
            return 1
    for form in compact_item_forms:
        if compact_query and compact_query in form:
            return 2

    address_forms = normalized_forms(item.address, item.subtitle)
    compact_address_forms = {compact_text(form) for form in address_forms if compact_text(form)}
    for form in compact_address_forms:
        if compact_query and compact_query in form:
            return 3

    if destination_context_match(item, request):
        return 4

    return 5


def locality_context_rank(item: ProviderPlace, request: PlaceSearchRequest) -> int:
    if destination_context_match(item, request):
        return 0
    return 1


def has_strong_preferred_match(
    items: list[ProviderPlace],
    request: PlaceSearchRequest,
    query_forms: set[str],
    preferred_codes: set[str],
) -> bool:
    if not preferred_codes:
        return False
    for item in items:
        if item.country_code.upper() not in preferred_codes:
            continue
        if query_match_rank(
            item,
            request,
            normalize(request.query),
            query_forms,
            normalized_forms(item.name, item.address, item.subtitle, item.locality),
        ) <= 4:
            return True
    return False


def has_strong_nominatim_match(
    items: list[ProviderPlace],
    request: PlaceSearchRequest,
    query_forms: set[str],
    preferred_codes: set[str],
) -> bool:
    for item in items:
        if item.provider != "nominatim":
            continue
        if preferred_codes and item.country_code.upper() not in preferred_codes:
            continue
        if query_match_rank(
            item,
            request,
            normalize(request.query),
            query_forms,
            normalized_forms(item.name, item.address, item.subtitle, item.locality),
        ) <= 4:
            return True
    return False


def compact_text(text: str) -> str:
    return normalize(text).replace(" ", "")


def destination_context_match(item: ProviderPlace, request: PlaceSearchRequest) -> bool:
    if not request.destination_context:
        return False

    candidate_forms = {
        compact_text(value)
        for value in (item.locality or "", item.subtitle or "", item.address or "", item.name or "")
        if compact_text(value)
    }
    if not candidate_forms:
        return False

    for destination in request.destination_context.destinations:
        destination_name = compact_text(destination.name)
        if not destination_name:
            continue
        if destination.country_code and item.country_code.upper() != destination.country_code.upper():
            continue
        for candidate in candidate_forms:
            if destination_name in candidate:
                return True
            if ngram_overlap(destination_name, candidate) >= 0.5:
                return True
    return False


def ngram_overlap(left: str, right: str) -> float:
    left_ngrams = character_ngrams(left)
    right_ngrams = character_ngrams(right)
    if not left_ngrams or not right_ngrams:
        return 0.0
    return len(left_ngrams & right_ngrams) / len(left_ngrams)


def character_ngrams(text: str) -> set[str]:
    if not text:
        return set()
    if len(text) == 1:
        return {text}
    return {text[index : index + 2] for index in range(len(text) - 1)}


# ── AI Itinerary Parsing ──

@app.post("/v1/ai/parse-itinerary", response_model=ParseItineraryResponse)
async def parse_itinerary_endpoint(request: ParseItineraryRequest) -> ParseItineraryResponse:
    if not settings.ai_parse_enabled:
        raise HTTPException(status_code=503, detail="AI parsing is disabled")

    from app.ai_service import parse_itinerary

    try:
        return await parse_itinerary(request.text, request.destination, request.language)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="AI service timeout")
    except httpx.HTTPStatusError as exc:
        logger.error("DeepSeek API error: %s %s", exc.response.status_code, exc.response.text[:300])
        raise HTTPException(status_code=502, detail=f"AI service error: {exc.response.status_code}")
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail="AI returned invalid JSON response")


@app.post("/v1/ai/parse-itinerary-no-geocoding", response_model=ParseItineraryResponseNoLocation)
async def parse_itinerary_no_geocoding_endpoint(request: ParseItineraryRequest) -> ParseItineraryResponseNoLocation:
    """Parse itinerary without backend geocoding - for client-side geocoding"""
    if not settings.ai_parse_enabled:
        raise HTTPException(status_code=503, detail="AI parsing is disabled")

    from app.ai_service_no_geocoding import parse_itinerary_no_geocoding

    try:
        # use_cache=False for testing phase
        return await parse_itinerary_no_geocoding(request.text, request.destination, request.language, use_cache=False)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="AI service timeout")
    except httpx.HTTPStatusError as exc:
        logger.error("DeepSeek API error: %s %s", exc.response.status_code, exc.response.text[:300])
        raise HTTPException(status_code=502, detail=f"AI service error: {exc.response.status_code}")
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail="AI returned invalid JSON response")


@app.post("/v1/ai/parse-itinerary-smart", response_model=ParseItinerarySmartResponse)
async def parse_itinerary_smart_endpoint(request: ParseItineraryRequest) -> ParseItinerarySmartResponse:
    if not settings.ai_parse_enabled:
        raise HTTPException(status_code=503, detail="AI parsing is disabled")

    from app.ai_service_smart import parse_itinerary_smart

    try:
        return await parse_itinerary_smart(request.text, request.destination, request.language)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="AI service timeout")
    except httpx.HTTPStatusError as exc:
        logger.error("DeepSeek API error: %s %s", exc.response.status_code, exc.response.text[:300])
        raise HTTPException(status_code=502, detail=f"AI service error: {exc.response.status_code}")
    except json.JSONDecodeError:
        raise HTTPException(status_code=502, detail="AI returned invalid JSON response")
