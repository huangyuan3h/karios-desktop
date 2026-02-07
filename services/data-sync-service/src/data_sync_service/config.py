from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

ROOT_ENV_PATH = Path(__file__).resolve().parents[4] / ".env"


def _load_env() -> None:
    if ROOT_ENV_PATH.exists():
        load_dotenv(ROOT_ENV_PATH)


@dataclass(frozen=True)
class Settings:
    database_url: str
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    _load_env()

    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_user = os.getenv("DB_USER", "admin")
    db_password = os.getenv("DB_PASSWORD", "admin123")
    db_name = os.getenv("DB_NAME", "karios-desktop")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    return Settings(
        database_url=database_url,
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
    )
