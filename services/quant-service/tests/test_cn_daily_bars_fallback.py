import types

from market import akshare_provider as p


class _FakeDf:
    def __init__(self, rows):
        self._rows = rows

    def to_dict(self, _orient):  # pandas-like
        return self._rows


def test_fetch_cn_a_daily_bars_falls_back_to_stock_zh_a_daily(monkeypatch) -> None:
    # Fake AkShare object: primary method fails, fallback method works.
    fake = types.SimpleNamespace()

    def _fail(**_k):
        raise RuntimeError("blocked/captcha")

    def _ok(**_k):
        return _FakeDf(
            [
                {"date": "2026-01-28", "open": 10, "high": 11, "low": 9, "close": 10, "volume": 100, "amount": 1000},
                {"date": "2026-01-29", "open": 11, "high": 12, "low": 10, "close": 11, "volume": 120, "amount": 1200},
            ]
        )

    fake.stock_zh_a_hist = _fail
    fake.stock_zh_a_daily = _ok

    monkeypatch.setattr(p, "_akshare", lambda: fake)

    bars = p.fetch_cn_a_daily_bars("002170", days=60)
    assert bars
    assert bars[-1].date == "2026-01-29"
    assert bars[-1].close == "11"

