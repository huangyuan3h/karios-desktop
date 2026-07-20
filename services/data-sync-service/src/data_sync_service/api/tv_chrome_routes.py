from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from data_sync_service.service import tv_chrome

router = APIRouter()


class TvChromeStartRequest(BaseModel):
    port: int = tv_chrome.TV_CDP_PORT_DEFAULT
    userDataDir: str = tv_chrome.TV_USER_DATA_DIR_DEFAULT
    profileDirectory: str = tv_chrome.TV_PROFILE_DIR_DEFAULT
    chromeBin: str = tv_chrome.TV_CHROME_BIN_DEFAULT
    headless: bool = False
    bootstrapFromChromeUserDataDir: str | None = None
    bootstrapFromProfileDirectory: str | None = None
    forceBootstrap: bool = False


@router.get("/integrations/tradingview/status")
def tradingview_status() -> dict:
    st = tv_chrome.status()
    return st.__dict__


@router.post("/integrations/tradingview/chrome/start")
def tradingview_chrome_start(req: TvChromeStartRequest) -> dict:
    st = tv_chrome.start(**req.model_dump())
    return st.__dict__


@router.post("/integrations/tradingview/chrome/stop")
def tradingview_chrome_stop() -> dict:
    st = tv_chrome.stop()
    return st.__dict__

