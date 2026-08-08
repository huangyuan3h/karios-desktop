"""em_push2_http shared HTTP helper coverage."""

from __future__ import annotations

import json
import urllib.error

from data_sync_service.service import em_push2_http as eh


def test_em_headers_valid_referer() -> None:
    h = eh._em_headers("https://quote.eastmoney.com/center/gridlist.html")
    assert h["Origin"] == "https://quote.eastmoney.com"
    assert h["Referer"] == "https://quote.eastmoney.com/center/gridlist.html"
    assert h["User-Agent"].startswith("Mozilla/5.0")


def test_em_headers_bad_referer() -> None:
    h = eh._em_headers("not-a-url")
    assert h["Origin"] == "https://quote.eastmoney.com"


def test_json_dict_from_text_empty() -> None:
    try:
        eh._json_dict_from_text("  ", source="x")
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "x_empty_body" in str(e)


def test_json_dict_from_text_invalid() -> None:
    try:
        eh._json_dict_from_text("{bad", source="curl")
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "curl_invalid_json" in str(e)


def test_json_dict_from_text_non_object() -> None:
    try:
        eh._json_dict_from_text("[1,2]", source="curl")
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "curl_non_object_json:list" in str(e)


def test_json_dict_from_text_ok() -> None:
    assert eh._json_dict_from_text('{"a": 1}', source="curl") == {"a": 1}


class _Proc:
    def __init__(self, returncode=0, stdout="", stderr="") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_curl_get_json_ok(monkeypatch) -> None:
    monkeypatch.setattr(eh.subprocess, "run", lambda args, **kw: _Proc(stdout='{"a":1}\n200'))
    out = eh._curl_get_json("https://push2.eastmoney.com/api", params={"p": "1"}, referer="https://quote.eastmoney.com", timeout=25.0)
    assert out == {"a": 1}


def test_curl_get_json_no_status(monkeypatch) -> None:
    monkeypatch.setattr(eh.subprocess, "run", lambda args, **kw: _Proc(stdout='{"a":1}'))
    assert eh._curl_get_json("https://x", params={}, referer="r", timeout=25.0) == {"a": 1}


def test_curl_get_json_nonzero(monkeypatch) -> None:
    monkeypatch.setattr(eh.subprocess, "run", lambda args, **kw: _Proc(returncode=7, stderr="conn refused"))
    try:
        eh._curl_get_json("https://x", params={}, referer="r", timeout=25.0)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "conn refused" in str(e)


def test_curl_get_json_http_error(monkeypatch) -> None:
    monkeypatch.setattr(eh.subprocess, "run", lambda args, **kw: _Proc(stdout="oops\n500"))
    try:
        eh._curl_get_json("https://x", params={}, referer="r", timeout=25.0)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "curl_http_500" in str(e)


class _Resp:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self._body = body
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def read(self):
        return self._body


def test_urllib_get_json_ok(monkeypatch) -> None:
    monkeypatch.setattr(eh.urllib.request, "urlopen", lambda req, timeout=25: _Resp(b'{"k":1}'))
    assert eh._urllib_get_json("https://push2", params={"a": "b"}, referer="r", timeout=25.0) == {"k": 1}


def test_urllib_get_json_http_error(monkeypatch) -> None:
    monkeypatch.setattr(eh.urllib.request, "urlopen", lambda req, timeout=25: _Resp(b"err", status=502))
    try:
        eh._urllib_get_json("https://push2", params={}, referer="r", timeout=25.0)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "urllib_http_502" in str(e)


def test_urllib_get_json_bad_body(monkeypatch) -> None:
    monkeypatch.setattr(eh.urllib.request, "urlopen", lambda req, timeout=25: _Resp(b"nope"))
    try:
        eh._urllib_get_json("https://push2", params={}, referer="r", timeout=25.0)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "urllib_invalid_json" in str(e)


def test_em_get_json_requests_path(monkeypatch) -> None:
    seen = {}

    class FakeResp:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return {"data": 1}

    fake_requests = type("requests", (), {"get": staticmethod(lambda *a, **kw: seen.update(kw) or FakeResp())})
    monkeypatch.setitem(eh.sys.modules if hasattr(eh, "sys") else __import__("sys").modules, "requests", fake_requests)
    out = eh.em_get_json("https://push2", params={"p": "1"}, referer="r")
    assert out == {"data": 1}
    assert seen["timeout"] == 25.0


def test_em_get_json_requests_non_object(monkeypatch) -> None:
    class R2:
        status_code = 200
        text = ""

        @staticmethod
        def json():
            return [1]

    monkeypatch.setitem(__import__("sys").modules, "requests", type("requests", (), {"get": staticmethod(lambda *a, **kw: R2())}))
    monkeypatch.setattr(eh, "_curl_get_json", lambda *a, **kw: {"from": "curl"})
    out = eh.em_get_json("https://push2", params={}, referer="r")
    assert out == {"from": "curl"}


def test_em_get_json_requests_http_error(monkeypatch) -> None:
    class R3:
        status_code = 429
        text = "too many"

    monkeypatch.setitem(__import__("sys").modules, "requests", type("requests", (), {"get": staticmethod(lambda *a, **kw: R3())}))
    monkeypatch.setattr(eh, "_curl_get_json", lambda *a, **kw: {"from": "curl"})
    assert eh.em_get_json("https://push2", params={}, referer="r") == {"from": "curl"}


def test_em_get_json_all_fallbacks_fail(monkeypatch) -> None:
    monkeypatch.setitem(__import__("sys").modules, "requests", None)
    monkeypatch.setattr(eh, "_curl_get_json", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("curl dead")))
    monkeypatch.setattr(eh, "_urllib_get_json", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("urllib dead")))
    try:
        eh.em_get_json("https://push2", params={}, referer="r")
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "curl dead" in str(e) and "urllib dead" in str(e)
        assert "requests" in str(e)
