"""EM upstream-unavailable degradation (2026-11-06).

Regression tests proving:
- Known East Money transport failures -> explicit degraded (ok=True),
  no fake zero rows, last-success never advanced (success=False audit row).
- Real internal failures -> still ok=False.
- Mixed runs continue; recovery works when the source recovers.

All network/DB touchpoints are mocked — no hanging EM requests, no Postgres.
"""

from __future__ import annotations

from data_sync_service.service import dashboard as dash
from data_sync_service.service import etf_fund_flow as eff
from data_sync_service.service.em_source_status import (
    is_em_source_unavailable,
    neutralize_em_substep,
)

# Exact banner shape seen on 2026-11-06 09:16:50 (screenshot).
BANNER_1106 = (
    "push2delay.eastmoney.com:direct:requests:('Connection aborted.', "
    "RemoteDisconnected('Remote end closed connection without response')); "
    "curl:curl: (52) Empty reply from server; "
    "urllib:Remote end closed connection without response; "
    "push2.eastmoney.com:direct:requests:('Connection aborted.', "
    "RemoteDisconnected('Remote end closed connection without response')); "
    "curl:curl: (52) Empty reply from server; "
    "urllib:Remote end closed connection without response"
)


class TestClassifier:
    def test_em_transport_banner_is_unavailable(self) -> None:
        assert is_em_source_unavailable(BANNER_1106) is True

    def test_ban_latch_is_unavailable(self) -> None:
        assert is_em_source_unavailable("eastmoney_ip_ban_latched") is True

    def test_em_chain_signature_is_unavailable(self) -> None:
        # Chain signature WITH EM attribution (screenshot shape, trimmed).
        assert (
            is_em_source_unavailable(
                "push2.eastmoney.com direct:requests timeout; curl:curl: (52) Empty reply; "
                "urllib:Remote end closed connection without response"
            )
            is True
        )

    def test_real_errors_stay_failures(self) -> None:
        assert is_em_source_unavailable("no_iv_data") is False
        assert is_em_source_unavailable("no realtime ETF flow rows from East Money") is False
        assert is_em_source_unavailable("suspicious_empty_lhb: source=tushare") is False
        assert is_em_source_unavailable("tushare: ReadTimeout: pool timeout") is False
        assert is_em_source_unavailable("") is False
        assert is_em_source_unavailable(None) is False
        assert is_em_source_unavailable({"ok": True}) is False

    def test_bare_host_mention_without_transport_stays_failed(self) -> None:
        # 2026-11-06 review: mere host mention (payload/schema failure naming
        # the URL) is NOT upstream-unavailable.
        assert is_em_source_unavailable("eastmoney.com response missing_symbols: 510300") is False
        assert is_em_source_unavailable("eastmoney_etf_no_rows: schema mismatch on diff") is False
        assert (
            is_em_source_unavailable("push2his.eastmoney.com returned empty diff: schema error")
            is False
        )
        assert (
            is_em_source_unavailable("push2delay.eastmoney.com invalid_json: programming bug")
            is False
        )

    def test_generic_chain_from_another_host_is_not_em(self) -> None:
        assert (
            is_em_source_unavailable(
                "tushare: direct:requests timeout; curl:curl: (52) Empty reply; "
                "urllib:timeout after 25s"
            )
            is False
        )

    def test_em_transport_plus_real_error_in_another_field_stays_failed(self) -> None:
        # AND semantics: one EM transport error must not mask a simultaneous
        # real failure carried in a separate field.
        assert (
            is_em_source_unavailable(
                {
                    "ok": False,
                    "error": BANNER_1106,
                    "errors": [{"error": "DB integrity violation: checksum mismatch"}],
                }
            )
            is False
        )
        assert (
            is_em_source_unavailable(
                {
                    "ok": False,
                    "error": BANNER_1106,
                    "providerErrors": ["tushare: quota exceeded"],
                }
            )
            is False
        )
        assert (
            is_em_source_unavailable(
                {"ok": False, "error": BANNER_1106, "historyError": "tushare: auth failed"}
            )
            is False
        )


class TestNeutralize:
    def test_em_failure_becomes_degraded(self) -> None:
        out = neutralize_em_substep({"ok": False, "error": BANNER_1106}, leg="etf_fund_flow")
        assert out is not None
        assert out["ok"] is True
        assert out["degraded"] is True and out["unavailable"] is True
        assert out["reason"] == "em_source_unavailable"
        assert "东方财富" in str(out["message"])
        # Raw transport text preserved for audit, not shown to users.
        assert "RemoteDisconnected" in str(out["rawError"])

    def test_real_failure_is_not_neutralized(self) -> None:
        assert (
            neutralize_em_substep({"ok": False, "error": "no_iv_data"}, leg="etf_fund_flow") is None
        )
        assert (
            neutralize_em_substep({"ok": False, "error": "tushare: ReadTimeout"}, leg="top_inst")
            is None
        )

    def test_success_passes_through(self) -> None:
        assert neutralize_em_substep({"ok": True}, leg="etf_fund_flow") is None


def _patch_etf_common(monkeypatch, *, fetch_error: str | None):
    """Mock every DB/network touchpoint of sync_etf_fund_flow_watchlist."""
    monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda force=False: False)
    monkeypatch.setattr(eff, "ensure_table", lambda: None)
    monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda symbols: {})
    monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: fetch_error)
    monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 0)
    upserted: list = []
    monkeypatch.setattr(
        eff, "upsert_daily_rows", lambda rows: (upserted.append(list(rows or [])), 0)[1]
    )
    records: list = []
    monkeypatch.setattr(
        eff,
        "insert_record",
        lambda job_type, success, last_ts_code=None, error_message=None: records.append(
            {"job_type": job_type, "success": success, "error_message": error_message}
        ),
    )
    return upserted, records


class TestEtfWatchlistDegraded:
    def test_em_outage_degrades_without_fake_rows(self, monkeypatch) -> None:
        upserted, records = _patch_etf_common(monkeypatch, fetch_error=BANNER_1106)
        out = eff.sync_etf_fund_flow_watchlist(force=True)
        assert out["ok"] is True
        assert out.get("degraded") is True and out.get("unavailable") is True
        assert "东方财富" in str(out.get("message"))
        assert upserted == [[]]  # no rows written — no fabricated zeros
        # Audit row stays failure so last-success never advances.
        assert len(records) == 1 and records[0]["success"] is False
        assert "em_source_unavailable" in str(records[0]["error_message"])

    def test_real_failure_stays_failed(self, monkeypatch) -> None:
        upserted, records = _patch_etf_common(monkeypatch, fetch_error=None)
        out = eff.sync_etf_fund_flow_watchlist(force=True)
        assert out["ok"] is False
        assert out.get("degraded") is not True
        assert records and records[0]["success"] is False

    def test_em_outage_plus_real_history_error_stays_failed(self, monkeypatch) -> None:
        upserted, records = _patch_etf_common(monkeypatch, fetch_error=BANNER_1106)

        def _boom(**kw):
            raise RuntimeError("tushare: auth failed")

        monkeypatch.setattr(eff, "_sync_tushare_history_if_available", _boom)
        out = eff.sync_etf_fund_flow_watchlist(force=True)
        assert out["ok"] is False
        assert out.get("degraded") is not True
        assert records and records[0]["success"] is False

    def test_recovery_returns_to_normal(self, monkeypatch) -> None:
        upserted, records = _patch_etf_common(monkeypatch, fetch_error=None)
        flow = {"dataDate": "2026-11-06", "netInflow": 1.5}
        monkeypatch.setattr(
            eff, "fetch_em_etf_realtime_flow_for_symbols", lambda symbols: {"510300": flow}
        )
        monkeypatch.setattr(
            eff,
            "_em_flow_to_daily_row",
            lambda ts_code, trade_date_iso, flow, updated_at: {
                "ts_code": ts_code,
                "trade_date": trade_date_iso,
                "net_inflow": 1.5,
            },
        )
        monkeypatch.setattr(eff, "_is_current_realtime_trade_date", lambda *a, **k: True)
        out = eff.sync_etf_fund_flow_watchlist(force=True)
        assert out["ok"] is True
        assert out.get("degraded") is not True
        assert records and records[0]["success"] is True


def _sentiment_ok():
    return {
        "ok": True,
        "asOfDate": "2026-11-05",
        "items": [{"date": "2026-11-05", "riskMode": "normal"}],
    }


class TestSentimentStep:
    def _patch(self, monkeypatch, *, sentiment=None, etf=None, top=None, iv=None):
        monkeypatch.setattr(
            dash, "sync_cn_sentiment", lambda date_str, force: sentiment or _sentiment_ok()
        )
        monkeypatch.setattr(
            dash,
            "sync_etf_fund_flow_watchlist",
            lambda force=False: etf or {"ok": True, "updated": 6},
        )
        monkeypatch.setattr(
            dash, "sync_top_inst_watchlist", lambda force=False: top or {"ok": True}
        )
        monkeypatch.setattr(dash, "sync_option_iv_daily", lambda force=False: iv or {"ok": True})

    def test_em_etf_outage_step_stays_ok_degraded(self, monkeypatch) -> None:
        self._patch(
            monkeypatch,
            etf={"ok": False, "error": BANNER_1106},
        )
        out = dash._sync_sentiment_step(force=True)
        assert out["ok"] is True
        assert out.get("degraded") is True
        assert any("东方财富" in w for w in out.get("warnings", []))
        # Raw diagnostics preserved in meta for audit.
        assert "RemoteDisconnected" in str(out.get("etfFundFlow", {}).get("rawError"))

    def test_new_degraded_etf_shape_propagates_warnings(self, monkeypatch) -> None:
        self._patch(
            monkeypatch,
            etf={
                "ok": True,
                "degraded": True,
                "unavailable": True,
                "reason": "em_source_unavailable",
                "message": "东方财富实时行情源暂时不可用",
                "warnings": ["etf_fund_flow：东方财富源暂时不可用，已沿用上次有效数据"],
            },
        )
        out = dash._sync_sentiment_step(force=True)
        assert out["ok"] is True
        assert out.get("degraded") is True
        assert any("东方财富" in w for w in out.get("warnings", []))

    def test_real_etf_failure_still_fails_step(self, monkeypatch) -> None:
        self._patch(monkeypatch, etf={"ok": False, "error": "tushare: ReadTimeout"})
        out = dash._sync_sentiment_step(force=True)
        assert out["ok"] is False

    def test_sentiment_compute_failure_stays_failed(self, monkeypatch) -> None:
        self._patch(
            monkeypatch,
            sentiment={"ok": False, "error": "trade calendar missing", "items": []},
            etf={"ok": False, "error": BANNER_1106},
        )
        out = dash._sync_sentiment_step(force=True)
        # Real compute failure must remain a failure even when EM also degrades.
        assert out["ok"] is False

    def test_mixed_tushare_and_em_top_inst_stays_failed(self, monkeypatch) -> None:
        self._patch(
            monkeypatch,
            top={
                "ok": False,
                "error": "tushare: quota exceeded; eastmoney: RemoteDisconnected",
                "providerErrors": [
                    "tushare: quota exceeded",
                    "eastmoney: RemoteDisconnected from push2.eastmoney.com",
                ],
            },
        )
        out = dash._sync_sentiment_step(force=True)
        assert out["ok"] is False

    def test_pure_em_top_inst_degrades(self, monkeypatch) -> None:
        self._patch(
            monkeypatch,
            top={
                "ok": False,
                "error": "eastmoney: RemoteDisconnected from push2.eastmoney.com",
                "providerErrors": ["eastmoney: RemoteDisconnected from push2.eastmoney.com"],
            },
        )
        out = dash._sync_sentiment_step(force=True)
        assert out["ok"] is True
        assert out.get("degraded") is True

    def test_em_transport_masking_integrity_error_stays_failed(self, monkeypatch) -> None:
        self._patch(
            monkeypatch,
            etf={
                "ok": False,
                "error": BANNER_1106,
                "errors": [{"error": "DB integrity violation: checksum mismatch"}],
            },
        )
        out = dash._sync_sentiment_step(force=True)
        assert out["ok"] is False
        assert out.get("degraded") is not True


class TestOverallSync:
    def test_mixed_run_continues_with_degradation(self, monkeypatch) -> None:
        degraded_sentiment = {
            "ok": True,
            "degraded": True,
            "warnings": ["etf_fund_flow：东方财富源暂时不可用，已沿用上次有效数据"],
        }
        monkeypatch.setattr(
            dash, "_sync_industry_step", lambda force=False: {"ok": True, "updated": 3}
        )
        monkeypatch.setattr(dash, "_sync_sentiment_step", lambda force: degraded_sentiment)
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True, "total": 9})
        out = dash.dashboard_sync_parallel(force=True)
        assert out["ok"] is True
        assert out.get("degraded") is True
        assert "东方财富" in str(out.get("degradedMessage"))
        assert any("东方财富" in w for w in out.get("warnings", []))
        # Other legs unaffected.
        by_name = {s["name"]: s for s in out["steps"]}
        assert by_name["industryFundFlow"]["ok"] is True
        assert by_name["macroDaily"]["ok"] is True

    def test_real_failure_still_fails_overall(self, monkeypatch) -> None:
        monkeypatch.setattr(
            dash, "_sync_industry_step", lambda force=False: {"ok": True, "updated": 3}
        )
        monkeypatch.setattr(
            dash,
            "_sync_sentiment_step",
            lambda force: {"ok": False, "error": "trade calendar missing"},
        )
        monkeypatch.setattr(dash, "_sync_macro_step", lambda: {"ok": True})
        monkeypatch.setattr(dash, "_sync_news_step", lambda: {"ok": True, "total": 9})
        out = dash.dashboard_sync_parallel(force=True)
        assert out["ok"] is False
        assert out.get("degraded") is not True
