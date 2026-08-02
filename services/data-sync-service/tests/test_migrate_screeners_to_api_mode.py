"""Tests for scripts/migrate_screeners_to_api_mode.py (OPT-057.x)."""

from __future__ import annotations

from unittest.mock import patch

from scripts import migrate_screeners_to_api_mode as m


def _run(argv: list[str]) -> int:
    return m.main(argv)


def test_dry_run_creates_nothing_in_db():
    """--dry-run should call fetch_screener_by_id (read) but never upsert."""
    with (
        patch.object(m.tvdb, "fetch_screener_by_id", return_value=None) as f_read,
        patch.object(m.tvdb, "upsert_screener") as f_write,
    ):
        rc = _run(["--dry-run"])
    assert rc == 0
    assert f_read.call_count == len(m.TEMPLATE_SCREENER_IDS)
    assert f_write.call_count == 0


def test_default_enabled_is_false():
    """Without --enable, new template rows must be enabled=False so the
    user can compare in Settings before activating."""
    with (
        patch.object(m.tvdb, "fetch_screener_by_id", return_value=None),
        patch.object(m.tvdb, "upsert_screener") as f_write,
    ):
        rc = _run([])
    assert rc == 0
    assert f_write.call_count == len(m.TEMPLATE_SCREENER_IDS)
    for call in f_write.call_args_list:
        kwargs = call.kwargs
        assert kwargs["enabled"] is False
        assert kwargs["mode"] == "api"
        assert kwargs["market"] in {"cn", "hk", "us"}
        assert isinstance(kwargs["filter_json"], list)
        assert isinstance(kwargs["api_columns"], list)


def test_enable_flag_sets_enabled_true():
    with (
        patch.object(m.tvdb, "fetch_screener_by_id", return_value=None),
        patch.object(m.tvdb, "upsert_screener") as f_write,
    ):
        rc = _run(["--enable"])
    assert rc == 0
    for call in f_write.call_args_list:
        assert call.kwargs["enabled"] is True


def test_idempotent_when_already_registered():
    """If all rows already exist with matching filter/market/mode, the
    script must NOT touch them (no upsert call)."""
    def fake_fetch(screener_id):
        # Return an existing row that matches the template exactly.
        for template_id, s_id in m.TEMPLATE_SCREENER_IDS.items():
            if s_id == screener_id:
                tpl = m.get_template(template_id)
                return {
                    "id": s_id,
                    "name": tpl.display_name,
                    "url": "",
                    "enabled": False,
                    "mode": "api",
                    "market": tpl.market,
                    "filterJson": list(tpl.filter_json),
                    "apiColumns": list(tpl.api_columns),
                }
        return None

    with (
        patch.object(m.tvdb, "fetch_screener_by_id", side_effect=fake_fetch),
        patch.object(m.tvdb, "upsert_screener") as f_write,
    ):
        rc = _run([])
    assert rc == 0
    assert f_write.call_count == 0


def test_updates_when_filter_changed():
    """If template filter_json changed in code, the script must upsert
    the new filter to the existing row (only that one — others stay noop)."""
    stale_filter = [{"left": "stale", "operation": "greater", "right": 0}]

    def fake_fetch(screener_id):
        for template_id, s_id in m.TEMPLATE_SCREENER_IDS.items():
            if s_id == screener_id:
                tpl = m.get_template(template_id)
                # Only the CN pullback row has a stale filter; others match.
                f = list(stale_filter) if s_id == "tmpl-pullback-v3-cn" else list(tpl.filter_json)
                return {
                    "id": s_id,
                    "name": tpl.display_name,
                    "url": "",
                    "enabled": False,
                    "mode": "api",
                    "market": tpl.market,
                    "filterJson": f,
                    "apiColumns": list(tpl.api_columns),
                }
        return None

    with (
        patch.object(m.tvdb, "fetch_screener_by_id", side_effect=fake_fetch),
        patch.object(m.tvdb, "upsert_screener") as f_write,
    ):
        rc = _run([])
    assert rc == 0
    # Exactly 1 write — only the stale row.
    assert f_write.call_count == 1
    upd = f_write.call_args_list[0]
    assert upd.kwargs["screener_id"] == "tmpl-pullback-v3-cn"
    # NB: Python identifier is `filter_json` (not `filterJson`); the DB
    # column is `filter_json` (snake_case). Filter is now an array.
    assert upd.kwargs["filter_json"][0]["left"] != "stale"
    assert upd.kwargs["mode"] == "api"


def test_exit_code_when_no_templates_registered():
    """Defensive: if templates list is empty (caller bug), exit 1."""
    with patch.object(m, "list_templates", return_value=()):
        rc = _run([])
    assert rc == 1


def test_exit_code_on_db_error():
    """If upsert raises, exit 2 (DB error)."""
    with (
        patch.object(m.tvdb, "fetch_screener_by_id", return_value=None),
        patch.object(m.tvdb, "upsert_screener", side_effect=RuntimeError("boom")),
    ):
        rc = _run([])
    assert rc == 2


def test_screener_ids_are_stable():
    """TEMPLATE_SCREENER_IDS keys/values must match all registered templates.

    If a template is added to tv/templates.py but not to this dict, the
    script silently skips it. Guard against drift.
    """
    template_ids = {t.template_id for t in m.list_templates()}
    assert set(m.TEMPLATE_SCREENER_IDS.keys()) == template_ids


def test_screener_ids_are_unique():
    """Screener id values must be unique (would collide on upsert otherwise)."""
    values = list(m.TEMPLATE_SCREENER_IDS.values())
    assert len(values) == len(set(values))