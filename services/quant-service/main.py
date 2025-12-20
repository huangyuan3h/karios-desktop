from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


@dataclass(frozen=True)
class ServerConfig:
    host: str
    port: int
    db_path: str


def load_config() -> ServerConfig:
    return ServerConfig(
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "4320")),
        db_path=os.getenv("DATABASE_PATH", str(Path(__file__).with_name("karios.sqlite3"))),
    )


app = FastAPI(title="Karios Quant Service", version="0.1.0")

# Local desktop app: keep it permissive for v0.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _connect() -> sqlite3.Connection:
    default_db = str(Path(__file__).with_name("karios.sqlite3"))
    db_path = os.getenv("DATABASE_PATH", default_db)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        )
        """,
    )
    conn.commit()
    return conn


def get_setting(key: str) -> str | None:
    with _connect() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])


def set_setting(key: str, value: str) -> None:
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )
        conn.commit()


class PortfolioSnapshotResponse(BaseModel):
    ok: bool
    message: str


class SystemPromptResponse(BaseModel):
    value: str


class SystemPromptRequest(BaseModel):
    value: str


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@app.get("/portfolio/snapshot", response_model=PortfolioSnapshotResponse)
def portfolio_snapshot() -> PortfolioSnapshotResponse:
    return PortfolioSnapshotResponse(ok=True, message="Not implemented yet.")


@app.get("/settings/system-prompt", response_model=SystemPromptResponse)
def get_system_prompt() -> SystemPromptResponse:
    value = get_setting("system_prompt") or ""
    return SystemPromptResponse(value=value)


@app.put("/settings/system-prompt")
def put_system_prompt(req: SystemPromptRequest) -> dict[str, bool]:
    set_setting("system_prompt", req.value)
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn

    config = load_config()
    uvicorn.run("main:app", host=config.host, port=config.port, reload=True)
