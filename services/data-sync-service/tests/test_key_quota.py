"""OPT-051 §12 #5: API Key parsing + per-key sliding-window quota.

Covers:
- env-var parser (legacy + new format)
- duplicate label / secret rejection
- sliding-window correctness (try_acquire, retry_after)
- QuotaTracker.check_and_record (raises 429 with proper headers)
- QuotaTracker.usage (snapshot)
- GET /v1/quota endpoint behaviour (auth off / on / per-key snapshot)
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.api.key_quota import (  # type: ignore[import-not-found]
    ApiKey,
    ApiKeyParseError,
    QuotaTracker,
    _Window,
    keys_from_env,
    parse_api_keys,
    quota_tracker,
)
from data_sync_service.main import app  # type: ignore[import-not-found]


client = TestClient(app)


@pytest.fixture(autouse=True)
def _reset_state() -> None:
    """Clear lru_cache + quota tracker state between tests."""
    from data_sync_service import config

    config.get_settings.cache_clear()
    quota_tracker.reset()
    yield
    config.get_settings.cache_clear()
    quota_tracker.reset()


# ---------------------------------------------------------------------------
# parse_api_keys
# ---------------------------------------------------------------------------


def test_parse_legacy_format_single_key() -> None:
    keys = parse_api_keys("sk-abc")
    assert len(keys) == 1
    assert keys[0].secret == "sk-abc"
    assert keys[0].rpm == 0 and keys[0].rph == 0 and keys[0].rpd == 0
    # Legacy key gets a label derived from the first 4 chars of the secret.
    assert keys[0].label == "key-sk-a"


def test_parse_legacy_format_multiple_keys() -> None:
    keys = parse_api_keys("sk-abc,sk-xyz")
    assert [k.secret for k in keys] == ["sk-abc", "sk-xyz"]


def test_parse_new_format_label_and_quota() -> None:
    keys = parse_api_keys("frontend:sk-abc:600:0:0")
    assert keys[0].label == "frontend"
    assert keys[0].secret == "sk-abc"
    assert keys[0].rpm == 600
    assert keys[0].rph == 0
    assert keys[0].rpd == 0


def test_parse_new_format_all_quotas() -> None:
    keys = parse_api_keys("external:sk-x:60:1000:10000")
    assert keys[0].rpm == 60
    assert keys[0].rph == 1000
    assert keys[0].rpd == 10000


def test_parse_duplicate_label_rejected() -> None:
    with pytest.raises(ApiKeyParseError, match="duplicate label"):
        parse_api_keys("frontend:sk-abc:0:0:0,frontend:sk-xyz:0:0:0")


def test_parse_duplicate_secret_rejected() -> None:
    with pytest.raises(ApiKeyParseError, match="duplicate secret"):
        parse_api_keys("alpha:sk-shared:0:0:0,beta:sk-shared:0:0:0")


def test_parse_negative_rpm_rejected() -> None:
    with pytest.raises(ApiKeyParseError, match="rpm must be"):
        parse_api_keys("a:b:-1:0:0")


def test_parse_non_integer_rph_rejected() -> None:
    with pytest.raises(ApiKeyParseError, match="rph must be int"):
        parse_api_keys("a:b:0:abc:0")


def test_parse_too_many_fields_rejected() -> None:
    with pytest.raises(ApiKeyParseError, match="1, 2, 3, 4, or 5"):
        parse_api_keys("a:b:c:d:e:f")


def test_parse_empty_string_returns_empty_list() -> None:
    assert parse_api_keys("") == []
    assert parse_api_keys(" , , ") == []


def test_keys_from_env_falls_back_to_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KARIOS_API_KEYS", raising=False)
    assert keys_from_env() == []


# ---------------------------------------------------------------------------
# _Window sliding-window correctness
# ---------------------------------------------------------------------------


def test_window_under_limit_allows() -> None:
    w = _Window(max_count=3, window_seconds=60)
    for _ in range(3):
        allowed, used, _ = w.try_acquire(now=1000.0)
        assert allowed is True
        assert used in (1, 2, 3)


def test_window_at_limit_rejects_with_retry_after() -> None:
    w = _Window(max_count=2, window_seconds=60)
    w.try_acquire(now=1000.0)
    w.try_acquire(now=1000.0)
    allowed, used, reset_in = w.try_acquire(now=1000.0)
    assert allowed is False
    assert used == 2
    assert reset_in >= 1


def test_window_zero_limit_short_circuits_to_allowed() -> None:
    w = _Window(max_count=0, window_seconds=60)
    # A 0-limit window means "unlimited" (per ApiKey.has_quota → skip tracking).
    allowed, used, _ = w.try_acquire(now=1000.0)
    assert allowed is True
    assert used == 0


def test_window_prunes_old_timestamps() -> None:
    w = _Window(max_count=2, window_seconds=60)
    w.try_acquire(now=900.0)
    w.try_acquire(now=950.0)
    # Move 100s forward; both prior hits are now outside the 60s window.
    allowed, used, _ = w.try_acquire(now=1050.0)
    assert allowed is True
    assert used == 1  # only the fresh hit counts


# ---------------------------------------------------------------------------
# QuotaTracker (process-wide)
# ---------------------------------------------------------------------------


def test_tracker_no_quota_short_circuits() -> None:
    k = ApiKey(label="a", secret="s", rpm=0, rph=0, rpd=0)
    tracker = QuotaTracker()
    for _ in range(1000):
        tracker.check_and_record(k)
    assert tracker.usage(k) == {}


def test_tracker_rpm_enforced() -> None:
    k = ApiKey(label="a", secret="s", rpm=2, rph=0, rpd=0)
    tracker = QuotaTracker()
    tracker.check_and_record(k)
    tracker.check_and_record(k)
    with pytest.raises(Exception) as exc_info:
        tracker.check_and_record(k)
    assert exc_info.value.status_code == 429
    assert "Retry-After" in exc_info.value.headers


def test_tracker_usage_returns_window_snapshots() -> None:
    k = ApiKey(label="a", secret="s", rpm=5, rph=100, rpd=1000)
    tracker = QuotaTracker()
    tracker.check_and_record(k)
    tracker.check_and_record(k)
    snap = tracker.usage(k)
    assert set(snap.keys()) == {"rpm", "rph", "rpd"}
    assert snap["rpm"]["used"] == 2
    assert snap["rpm"]["limit"] == 5
    assert snap["rph"]["limit"] == 100


# ---------------------------------------------------------------------------
# GET /v1/quota endpoint
# ---------------------------------------------------------------------------


def test_quota_anonymous_when_auth_disabled() -> None:
    # KARIOS_API_KEYS is unset by default → auth disabled.
    resp = client.get("/v1/quota")
    assert resp.status_code == 200
    body = resp.json()
    assert body["key_label"] == "anonymous"
    assert body["auth_enabled"] is False
    assert body["windows"] == {}


def test_quota_requires_auth_when_keys_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KARIOS_API_KEYS", "frontend:sk-abc:60:0:0")
    import data_sync_service.api.key_quota as kq

    kq.quota_tracker.reset()
    # Rebuild app.state (we cannot easily restart the app here, so we set
    # state directly — this mirrors what `main.py` does at startup).
    app.state.api_keys = kq.keys_from_env()

    resp = client.get("/v1/quota")
    assert resp.status_code == 401


def test_quota_returns_matched_key_usage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KARIOS_API_KEYS", "frontend:sk-abc:60:1000:0,external:sk-xyz:10:0:0")
    import data_sync_service.api.key_quota as kq

    kq.quota_tracker.reset()
    app.state.api_keys = kq.keys_from_env()

    resp = client.get("/v1/quota", headers={"Authorization": "Bearer sk-xyz"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["key_label"] == "external"
    assert body["auth_enabled"] is True
    assert body["windows"]["rpm"]["limit"] == 10
    # Frontend's rph/rpd is NOT in the response because limit=0 windows are
    # omitted by the tracker.
    assert "rph" not in body["windows"]
    assert "rpd" not in body["windows"]


def test_quota_429_when_exceeded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KARIOS_API_KEYS", "test:sk-abc:2:0:0")
    import data_sync_service.api.key_quota as kq

    kq.quota_tracker.reset()
    app.state.api_keys = kq.keys_from_env()

    h = {"Authorization": "Bearer sk-abc"}
    assert client.get("/v1/quota", headers=h).status_code == 200
    assert client.get("/v1/quota", headers=h).status_code == 200
    resp = client.get("/v1/quota", headers=h)
    assert resp.status_code == 429
    assert "Retry-After" in resp.headers
    assert resp.headers["X-RateLimit-Limit"] == "2"


def test_quota_per_key_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Key A burning its budget does not affect key B's window."""
    monkeypatch.setenv("KARIOS_API_KEYS", "a:sk-a:2:0:0,b:sk-b:2:0:0")
    import data_sync_service.api.key_quota as kq

    kq.quota_tracker.reset()
    app.state.api_keys = kq.keys_from_env()

    h_a = {"Authorization": "Bearer sk-a"}
    h_b = {"Authorization": "Bearer sk-b"}
    client.get("/v1/quota", headers=h_a)
    client.get("/v1/quota", headers=h_a)
    # A is exhausted.
    assert client.get("/v1/quota", headers=h_a).status_code == 429
    # B is still fresh.
    assert client.get("/v1/quota", headers=h_b).status_code == 200