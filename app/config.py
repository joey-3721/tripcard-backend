from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "TripCard Backend"
    app_host: str = "0.0.0.0"
    app_port: int = 2778
    log_level: str = "INFO"

    user_agent: str = "TripCardBackend/1.0 (self-hosted)"
    request_timeout_seconds: float = 8.0

    enable_nominatim: bool = True
    enable_photon: bool = True
    enable_mapkit_proxy: bool = False

    nominatim_base_url: str = "https://nominatim.openstreetmap.org/search"
    photon_base_url: str = "https://photon.komoot.io/api"

    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = ""
    mysql_password: str = ""
    mysql_db: str = "travel"

    cache_enabled: bool = False
    cache_ttl_seconds: int = 60 * 60 * 24
    cache_table_name: str = "place_search_cache"

    cors_allow_origins: list[str] = ["*"]

    ai_parse_enabled: bool = True
    ai_tokens_table_name: str = "ai_tokens"
    ai_request_timeout_seconds: float = 60.0
    ai_max_input_length: int = 10000
    ai_geocode_concurrency: int = 5

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
