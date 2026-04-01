from __future__ import annotations

import hashlib
import json
import logging
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.cache_mysql import MySQLCache
from app.config import settings
from app.providers.base import ProviderPlace
from app.providers.nominatim import search_nominatim
from app.providers.photon import search_photon
from app.schemas import PlaceResult, PlaceSearchMeta, PlaceSearchRequest, PlaceSearchResponse

logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
logger = logging.getLogger("tripcard-backend")
cache = MySQLCache() if settings.cache_enabled else None


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("starting backend")
    if cache is not None:
        cache.ensure_table()
        cache.cleanup()
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
        for candidate in context_queries:
            if settings.enable_nominatim:
                try:
                    merged.extend(
                        await search_nominatim(
                            client,
                            candidate,
                            request.language,
                            request.country_filter_code,
                            request.limit,
                        )
                    )
                    if "nominatim" not in providers_used:
                        providers_used.append("nominatim")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("nominatim failed query=%s error=%r", candidate, exc)

            if settings.enable_photon and len(merged) < max(request.limit, 8):
                try:
                    merged.extend(
                        await search_photon(
                            client,
                            candidate,
                            request.language,
                            request.country_filter_code,
                            request.limit,
                        )
                    )
                    if "photon" not in providers_used:
                        providers_used.append("photon")
                except Exception as exc:  # noqa: BLE001
                    logger.warning("photon failed query=%s error=%r", candidate, exc)

    results = rank_and_convert(merged, request)
    response = PlaceSearchResponse(
        query=request.query,
        trace_id=trace_id,
        results=results[: request.limit],
        meta=PlaceSearchMeta(
            scope=request.scope,
            country_filter_code=request.country_filter_code,
            preferred_country_codes=request.preferred_country_codes,
            fallback_used="photon" in providers_used,
            providers_used=providers_used,
            cache_hit=False,
            self_hosted_data=False,
        ),
    )
    if cache is not None:
        cache.set(
            cache_key,
            json.loads(response.model_dump_json()),
            settings.cache_ttl_seconds,
        )
    return response


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


def build_cache_key(request: PlaceSearchRequest) -> str:
    payload = json.dumps(request.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def rank_and_convert(items: list[ProviderPlace], request: PlaceSearchRequest) -> list[PlaceResult]:
    preferred_codes = {code.upper() for code in request.preferred_country_codes if code}
    seen: set[str] = set()
    results: list[PlaceResult] = []
    normalized_query = normalize(request.query)

    scored = sorted(
        items,
        key=lambda item: ranking_tuple(item, normalized_query, preferred_codes, request.country_filter_code),
    )

    for item in scored:
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
                score=score_value(item, normalized_query, preferred_codes),
                matched_by=matched_by(item, normalized_query, preferred_codes),
            )
        )
    return results


def ranking_tuple(
    item: ProviderPlace,
    normalized_query: str,
    preferred_codes: set[str],
    country_filter_code: str | None,
) -> tuple[int, int, int, int, str]:
    code = item.country_code.upper()
    normalized_name = normalize(item.name)
    normalized_address = normalize(item.address or "")

    if country_filter_code and code == country_filter_code.upper():
        filter_rank = 0
    elif country_filter_code:
        filter_rank = 1
    else:
        filter_rank = 0

    if normalized_name == normalized_query:
        query_rank = 0
    elif normalized_name.startswith(normalized_query):
        query_rank = 1
    elif normalized_query in normalized_name:
        query_rank = 2
    elif normalized_query in normalized_address:
        query_rank = 3
    else:
        query_rank = 4

    preferred_rank = 0 if preferred_codes and code in preferred_codes else 1
    address_rank = 0 if item.address else 1
    return (filter_rank, query_rank, preferred_rank, address_rank, normalized_name)


def score_value(item: ProviderPlace, normalized_query: str, preferred_codes: set[str]) -> float:
    score = 0.5
    name = normalize(item.name)
    if name == normalized_query:
        score += 0.35
    elif name.startswith(normalized_query):
        score += 0.25
    elif normalized_query in name:
        score += 0.15
    if preferred_codes and item.country_code.upper() in preferred_codes:
        score += 0.12
    if item.address:
        score += 0.03
    return min(score, 0.99)


def matched_by(item: ProviderPlace, normalized_query: str, preferred_codes: set[str]) -> list[str]:
    marks: list[str] = []
    name = normalize(item.name)
    if name == normalized_query:
        marks.append("name_exact")
    elif name.startswith(normalized_query):
        marks.append("name_prefix")
    elif normalized_query in name:
        marks.append("name_contains")
    if preferred_codes and item.country_code.upper() in preferred_codes:
        marks.append("preferred_country_boost")
    if item.address:
        marks.append("has_address")
    return marks


def normalize(text: str) -> str:
    return " ".join(text.lower().strip().split())
