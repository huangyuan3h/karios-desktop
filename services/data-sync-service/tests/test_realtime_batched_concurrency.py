from __future__ import annotations

import threading
import time
from unittest.mock import patch

from data_sync_service.service.realtime_quote import fetch_realtime_quotes_batched


def test_fetch_realtime_quotes_batched_runs_concurrent_batches() -> None:
    inflight = 0
    max_inflight = 0
    lock = threading.Lock()

    def _fake_fetch(codes: list[str]) -> dict:
        nonlocal inflight, max_inflight
        with lock:
            inflight += 1
            max_inflight = max(max_inflight, inflight)
        time.sleep(0.05)
        with lock:
            inflight -= 1
        return {
            "ok": True,
            "items": [{"ts_code": c, "price": "1.0"} for c in codes],
        }

    codes = [f"{i:06d}.SZ" for i in range(120)]
    with patch(
        "data_sync_service.service.realtime_quote.fetch_realtime_quotes",
        side_effect=_fake_fetch,
    ):
        items = fetch_realtime_quotes_batched(codes, batch_size=50, max_workers=6)

    assert len(items) == 120
    assert max_inflight > 1
