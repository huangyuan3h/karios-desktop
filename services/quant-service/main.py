from __future__ import annotations

import os
from dataclasses import dataclass

from fastapi import FastAPI
from pydantic import BaseModel


@dataclass(frozen=True)
class ServerConfig:
    host: str
    port: int


def load_config() -> ServerConfig:
    return ServerConfig(
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "4320")),
    )


app = FastAPI(title="Karios Quant Service", version="0.1.0")


class PortfolioSnapshotResponse(BaseModel):
    ok: bool
    message: str


@app.get("/healthz")
def healthz() -> dict[str, bool]:
    return {"ok": True}


@app.get("/portfolio/snapshot", response_model=PortfolioSnapshotResponse)
def portfolio_snapshot() -> PortfolioSnapshotResponse:
    return PortfolioSnapshotResponse(ok=True, message="Not implemented yet.")


if __name__ == "__main__":
    import uvicorn

    config = load_config()
    uvicorn.run("main:app", host=config.host, port=config.port, reload=True)
