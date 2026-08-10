from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

ROOT_ENV_PATH = Path(__file__).resolve().parents[4] / ".env"


def _load_env() -> None:
    if ROOT_ENV_PATH.exists():
        # override=True: the repo root .env is the single source of truth.
        # Without it, a stale/empty TU_SHARE_API_KEY in the parent shell env
        # shadows the .env value (observed 2026-08-10: hk_basic_sync /
        # macro_daily silently empty-keyed despite .env being correct).
        load_dotenv(ROOT_ENV_PATH, override=True)


@dataclass(frozen=True)
class Settings:
    database_url: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str
    tu_share_api_key: str
    ai_service_base_url: str
    # OPT-045: OpenAI 兼容 /v1/* + AI 助手可发现性
    karios_api_version: str
    karios_api_keys: tuple[str, ...]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    _load_env()

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_user = os.getenv("DB_USER", "admin")
    db_password = os.getenv("DB_PASSWORD", "admin123")
    db_name = os.getenv("DB_NAME", "karios-desktop")
    tu_share_api_key = os.getenv("TU_SHARE_API_KEY", "")
    ai_service_base_url = os.getenv("AI_SERVICE_BASE_URL", "http://127.0.0.1:4310").rstrip("/")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # OPT-045: API version + optional API Key list (comma-separated, empty = auth disabled).
    karios_api_version = os.getenv("KARIOS_API_VERSION", "0.1.0")
    karios_api_keys_raw = os.getenv("KARIOS_API_KEYS", "").strip()
    karios_api_keys = tuple(k for k in karios_api_keys_raw.split(",") if k.strip())

    return Settings(
        database_url=database_url,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        tu_share_api_key=tu_share_api_key,
        ai_service_base_url=ai_service_base_url,
        karios_api_version=karios_api_version,
        karios_api_keys=karios_api_keys,
    )
