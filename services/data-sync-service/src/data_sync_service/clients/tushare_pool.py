"""Pooled Tushare access: multi-token round-robin + quota watchdog (OPT-124).

Background: every close-chain service built its own ``ts.pro_api(single_key)``,
so one ``200/min`` burst (fund_daily full sync) or the Friday double-job
collision took the whole EOD chain down with a bare ``频率超限``. This module
is the single convergence point (anti-pattern: per-service ``pro_api`` calls):

- ``TusharePool`` holds N tokens, deals calls round-robin, and tracks a
  per-key 60s sliding window (default 200 calls/min).
- On a rate-limit signal the pool rotates to the next key and retries; only
  when every key is hot does it ``sleep(35)`` once and retry (the established
  ``cn_extra_sync`` idiom), bounded to two full rounds before re-raising.
- Endpoints with a daily budget (``index_global`` 100/day) raise
  :class:`DailyQuotaExhausted` once all keys are spent so callers can degrade
  to Tencent/akshare instead of burning retries.
- Everything is injectable (``pro_factory`` / ``clock`` / ``sleep`` /
  ``today``) so the whole policy is unit-testable without network.

Thread-safety: scheduler jobs run in threads; all mutable state is guarded
by one lock (same precedent as ``realtime_quote._TUSHARE_TOKEN_LOCK``).
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from collections.abc import Callable
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_SHANGHAI = ZoneInfo("Asia/Shanghai")

#: Substrings (lower-cased) in an exception message that mean "this key is
#: rate-limited — rotate, don't count it as a hard failure".
RATE_LIMIT_SIGNALS: tuple[str, ...] = (
    "频率超限",
    "rate limit",
    "ratelimit",
    "too many requests",
    "429",
    "访问频次",
    "调用次数",
    "超出",
)

#: Default per-key, per-endpoint budget: calls per rolling 60s.
DEFAULT_CALLS_PER_MINUTE = 200

#: Endpoint-specific per-minute budgets (tighter than the default).
PER_ENDPOINT_CALLS_PER_MINUTE: dict[str, int] = {
    # hk_daily is ~1 call/min on lower-tier keys; it is a last-resort
    # single-ticker fallback, so a 60s spacing only binds in pathological loops.
    "hk_daily": 1,
}

#: Endpoints with a per-key daily budget: endpoint -> calls per calendar day.
DAILY_LIMITED_ENDPOINTS: dict[str, int] = {
    # index_global is capped at 100 calls/day on lower-tier keys (observed
    # 2026-08-10: HSI/HSTECH stopped updating mid-week).
    "index_global": 100,
}

#: Back-off when every key is hot (established cn_extra_sync idiom).
ALL_KEYS_HOT_SLEEP_SECONDS = 35.0

#: Full rotation rounds before a persistent rate limit is re-raised instead
#: of retried (callers such as close_sync have their own outer retry).
MAX_ROTATION_ROUNDS = 2


class DailyQuotaExhausted(RuntimeError):
    """All pool keys spent their daily budget for one endpoint.

    Callers should degrade to a non-tushare source (Tencent/akshare/yfinance)
    instead of retrying — a retry cannot succeed before the next calendar day.
    """

    def __init__(self, endpoint: str, limit: int) -> None:
        super().__init__(f"tushare daily quota exhausted for {endpoint} ({limit}/day on every key)")
        self.endpoint = endpoint
        self.limit = limit


def is_rate_limit_error(exc: BaseException) -> bool:
    """True when an exception message signals a key-level rate limit."""
    msg = str(exc).lower()
    return any(sig in msg for sig in RATE_LIMIT_SIGNALS)


def _today_shanghai() -> date:
    from datetime import datetime

    return datetime.now(_SHANGHAI).date()


class TusharePool:
    """Round-robin pool over N tushare tokens with quota watchdog."""

    def __init__(
        self,
        tokens: list[str] | tuple[str, ...],
        *,
        pro_factory: Callable[[str], Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        today: Callable[[], date] = _today_shanghai,
    ) -> None:
        clean = [t.strip() for t in tokens if t and t.strip()]
        if not clean:
            raise ValueError("TusharePool needs at least one token")
        self._tokens = tuple(clean)
        self._clock = clock
        self._sleep = sleep
        self._today = today
        if pro_factory is None:
            import tushare as ts  # type: ignore[import-not-found]

            pro_factory = ts.pro_api
        self._pro_factory = pro_factory
        self._lock = threading.Lock()
        self._pros: dict[str, Any] = {}
        self._cursor = 0
        # per-key rolling-60s call timestamps (all endpoints share the key budget).
        self._minute_hits: dict[str, deque[float]] = {t: deque() for t in self._tokens}
        # per-endpoint spacing: (key, endpoint) -> last call monotonic ts.
        self._last_call: dict[tuple[str, str], float] = {}
        # per-key cooling deadline after a rate-limit signal.
        self._cool_until: dict[str, float] = {}
        # daily budgets: (key, endpoint) -> [day, used].
        self._daily_used: dict[tuple[str, str], list[Any]] = {}
        self._rotations = 0

    @property
    def tokens(self) -> tuple[str, ...]:
        return self._tokens

    @property
    def rotation_count(self) -> int:
        """How many times a hot key forced a rotation (observability)."""
        with self._lock:
            return self._rotations

    # -- internals (caller must hold _lock unless noted) --------------------

    def _prune(self, key: str, now: float) -> None:
        hits = self._minute_hits[key]
        while hits and hits[0] <= now - 60.0:
            hits.popleft()

    def _minute_budget(self, endpoint: str) -> int:
        return PER_ENDPOINT_CALLS_PER_MINUTE.get(endpoint, DEFAULT_CALLS_PER_MINUTE)

    def _key_ready_in(self, key: str, endpoint: str, now: float) -> float:
        """Seconds until `key` may serve `endpoint` (0 = now)."""
        cool = self._cool_until.get(key, 0.0)
        if cool > now:
            return cool - now
        self._prune(key, now)
        budget = self._minute_budget(endpoint)
        hits = self._minute_hits[key]
        if len(hits) >= budget:
            return max(0.0, hits[0] + 60.0 - now)
        spacing = 60.0 / budget if budget < DEFAULT_CALLS_PER_MINUTE else 0.0
        if spacing:
            last = self._last_call.get((key, endpoint), 0.0)
            wait = last + spacing - now
            if wait > 0:
                return wait
        return 0.0

    def _daily_spent(self, key: str, endpoint: str) -> bool:
        limit = DAILY_LIMITED_ENDPOINTS.get(endpoint)
        if not limit:
            return False
        today = self._today()
        rec = self._daily_used.get((key, endpoint))
        if rec is None or rec[0] != today:
            return False
        return int(rec[1]) >= limit

    def _record_call(self, key: str, endpoint: str, now: float) -> None:
        self._minute_hits[key].append(now)
        self._last_call[(key, endpoint)] = now
        if endpoint in DAILY_LIMITED_ENDPOINTS:
            today = self._today()
            rec = self._daily_used.get((key, endpoint))
            if rec is None or rec[0] != today:
                self._daily_used[(key, endpoint)] = [today, 1]
            else:
                rec[1] = int(rec[1]) + 1

    def _pro_for(self, key: str) -> Any:
        pro = self._pros.get(key)
        if pro is None:
            pro = self._pro_factory(key)
            self._pros[key] = pro
        return pro

    # -- public API ----------------------------------------------------------

    def call(self, endpoint: str, *args: Any, **kwargs: Any) -> Any:
        """Call one tushare pro endpoint through the pool.

        Rotates keys on rate-limit signals, waits for quota otherwise, and
        raises :class:`DailyQuotaExhausted` when a daily budget is spent on
        every key.
        """
        tried_this_round: set[str] = set()
        rounds = 0
        last_error: Exception | None = None
        pro: Any = None
        while True:
            with self._lock:
                now = self._clock()
                # Prefer keys with immediate capacity, round-robin order.
                ordered = [
                    self._tokens[(self._cursor + i) % len(self._tokens)]
                    for i in range(len(self._tokens))
                ]
                candidate: str | None = None
                wait_min = float("inf")
                for key in ordered:
                    if key in tried_this_round and len(tried_this_round) < len(self._tokens):
                        continue
                    if self._daily_spent(key, endpoint):
                        continue
                    wait = self._key_ready_in(key, endpoint, now)
                    if wait <= 0:
                        candidate = key
                        break
                    wait_min = min(wait_min, wait)
                if candidate is None:
                    if all(self._daily_spent(k, endpoint) for k in self._tokens):
                        raise DailyQuotaExhausted(endpoint, DAILY_LIMITED_ENDPOINTS[endpoint])
                    # Every usable key needs a wait: sleep until the earliest
                    # slot frees, then re-evaluate (no busy loop).
                    nap = min(wait_min, 65.0) if wait_min != float("inf") else 1.0
                    nap = max(nap, 0.05)
                    self._cursor = (self._cursor + 1) % len(self._tokens)
                    sleep_for, record_key = nap, None
                else:
                    self._record_call(candidate, endpoint, now)
                    self._cursor = (self._tokens.index(candidate) + 1) % len(self._tokens)
                    pro, sleep_for, record_key = self._pro_for(candidate), 0.0, candidate
            if record_key is None:
                self._sleep(sleep_for)
                continue
            try:
                return getattr(pro, endpoint)(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 — signal sniffing is the point
                if not is_rate_limit_error(exc):
                    raise
                last_error = exc
                with self._lock:
                    self._rotations += 1
                    self._cool_until[record_key] = self._clock() + ALL_KEYS_HOT_SLEEP_SECONDS
                    tried_this_round.add(record_key)
                    logger.warning(
                        "tushare pool: key …%s hot on %s (%s); rotating",
                        record_key[-4:],
                        endpoint,
                        str(exc)[:100],
                    )
                    if len(tried_this_round) >= len(self._tokens):
                        rounds += 1
                        if rounds >= MAX_ROTATION_ROUNDS:
                            break
                        # All keys hot: one back-off, then a fresh round.
                        tried_this_round.clear()
                        nap = ALL_KEYS_HOT_SLEEP_SECONDS
                    else:
                        nap = 0.0
                if nap:
                    self._sleep(nap)
                continue
        assert last_error is not None
        raise last_error

    def pro(self) -> PooledPro:
        """Drop-in ``ts.pro_api`` replacement: ``pool.pro().daily(...)``."""
        return PooledPro(self)

    def snapshot(self) -> dict[str, Any]:
        """JSON-safe quota snapshot for ``GET /api/health/datasources``.

        Never raises: the health endpoint must not fail because quota
        introspection did.
        """
        try:
            with self._lock:
                now = self._clock()
                keys = []
                for i, key in enumerate(self._tokens):
                    self._prune(key, now)
                    keys.append(
                        {
                            "index": i,
                            "suffix": key[-4:] if len(key) >= 4 else "****",
                            "minuteUsed": len(self._minute_hits[key]),
                            "minuteLimit": DEFAULT_CALLS_PER_MINUTE,
                            "coolingSeconds": max(0, int(self._cool_until.get(key, 0.0) - now)),
                        }
                    )
                daily = {
                    ep: {
                        "limit": lim,
                        "usedTotal": sum(
                            int(r[1])
                            for (k, e), r in self._daily_used.items()
                            if e == ep and r[0] == self._today()
                        ),
                    }
                    for ep, lim in DAILY_LIMITED_ENDPOINTS.items()
                }
                return {
                    "configured": True,
                    "keyCount": len(self._tokens),
                    "rotations": self._rotations,
                    "keys": keys,
                    "daily": daily,
                }
        except Exception:  # noqa: BLE001
            return {"configured": False}


class PooledPro:
    """Attribute-routed facade: ``pool.pro().<endpoint>(**kw)`` → ``pool.call``."""

    __slots__ = ("_pool",)

    def __init__(self, pool: TusharePool) -> None:
        object.__setattr__(self, "_pool", pool)

    def __getattr__(self, endpoint: str) -> Callable[..., Any]:
        pool = object.__getattribute__(self, "_pool")

        def _call(*args: Any, **kwargs: Any) -> Any:
            return pool.call(endpoint, *args, **kwargs)

        return _call


_POOL: TusharePool | None = None
_POOL_TOKENS: tuple[str, ...] = ()


def get_pool() -> TusharePool:
    """Process-wide pool built from settings (rebuilt when tokens change).

    Reads settings lazily so tests can monkeypatch
    ``data_sync_service.config.get_settings`` and pool-using services pick the
    change up without a restart (also covers runtime .env edits).
    """
    global _POOL, _POOL_TOKENS
    from data_sync_service.config import get_settings

    tokens = get_settings().tushare_tokens
    if _POOL is None or tokens != _POOL_TOKENS:
        if not tokens:
            raise RuntimeError("TU_SHARE_API_KEY is not set")
        _POOL = TusharePool(list(tokens))
        _POOL_TOKENS = tokens
    return _POOL


def reset_pool() -> None:
    """Drop the process-wide pool (test helper)."""
    global _POOL, _POOL_TOKENS
    _POOL = None
    _POOL_TOKENS = ()


def pool_snapshot() -> dict[str, Any]:
    """Best-effort snapshot for the health endpoint (never raises)."""
    try:
        return get_pool().snapshot()
    except Exception:  # noqa: BLE001 — unconfigured token etc.
        return {"configured": False}
