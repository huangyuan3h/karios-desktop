"""sina_http shared helper coverage."""

from __future__ import annotations

import sys

from data_sync_service.service import sina_http as sh


def test_sina_headers() -> None:
    h = sh._sina_headers()
    assert h["Referer"] == "https://finance.sina.com.cn/"
    h2 = sh._sina_headers("https://x.com")
    assert h2["Referer"] == "https://x.com"


def test_requests_get_ok_gbk(monkeypatch) -> None:
    class R:
        status_code = 200
        encoding = None
        text = "var hq_str_hk00700=\"a,b\""

    monkeypatch.setitem(sys.modules, "requests", type("requests", (), {"get": staticmethod(lambda *a, **kw: R())}))
    out = sh._requests_get("https://hq.sinajs.cn", timeout=5.0)
    assert out == 'var hq_str_hk00700="a,b"'
    assert out.strip()  # body returned


def test_requests_get_ok_utf8_kept(monkeypatch) -> None:
    class R:
        status_code = 200
        encoding = "utf-8"
        text = "x"

    monkeypatch.setitem(sys.modules, "requests", type("requests", (), {"get": staticmethod(lambda *a, **kw: R())}))
    assert sh._requests_get("https://hq.sinajs.cn", timeout=5.0) == "x"
    assert R.encoding == "utf-8"


def test_requests_get_http_error(monkeypatch) -> None:
    class R:
        status_code = 403
        text = "forbidden"

    monkeypatch.setitem(sys.modules, "requests", type("requests", (), {"get": staticmethod(lambda *a, **kw: R())}))
    try:
        sh._requests_get("https://hq.sinajs.cn", timeout=5.0)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "http_403" in str(e)


class _Resp:
    def __init__(self, raw: bytes, status: int = 200) -> None:
        self._raw = raw
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def read(self):
        return self._raw


def test_urllib_get_ok(monkeypatch) -> None:
    monkeypatch.setattr(sh.urllib.request, "urlopen", lambda req, timeout=10: _Resp("中文内容".encode("gb18030")))
    out = sh._urllib_get("https://hq.sinajs.cn", timeout=10.0)
    assert out == "中文内容"


def test_urllib_get_http_error(monkeypatch) -> None:
    monkeypatch.setattr(sh.urllib.request, "urlopen", lambda req, timeout=10: _Resp(b"err", status=500))
    try:
        sh._urllib_get("https://hq.sinajs.cn", timeout=10.0)
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "urllib_http_500" in str(e)


def test_sina_get_text_requests_path(monkeypatch) -> None:
    monkeypatch.setattr(sh, "_requests_get", lambda url, timeout=10.0: "body")
    monkeypatch.setattr(sh, "_urllib_get", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("not used")))
    assert sh.sina_get_text("https://hq.sinajs.cn") == "body"


def test_sina_get_text_fallback(monkeypatch) -> None:
    monkeypatch.setattr(sh, "_requests_get", lambda url, timeout=10.0: (_ for _ in ()).throw(RuntimeError("r dead")))
    monkeypatch.setattr(sh, "_urllib_get", lambda url, timeout=10.0: "urllib body")
    assert sh.sina_get_text("https://hq.sinajs.cn") == "urllib body"


def test_sina_get_text_all_fail(monkeypatch) -> None:
    monkeypatch.setattr(sh, "_requests_get", lambda url, timeout=10.0: (_ for _ in ()).throw(RuntimeError("r dead")))
    monkeypatch.setattr(sh, "_urllib_get", lambda url, timeout=10.0: (_ for _ in ()).throw(RuntimeError("u dead")))
    try:
        sh.sina_get_text("https://hq.sinajs.cn")
        raise AssertionError("expected RuntimeError")
    except RuntimeError as e:
        assert "r dead" in str(e) and "u dead" in str(e)


def test_build_hq_url() -> None:
    url = sh.build_hq_url(["00700", "09988"])
    assert url == "https://hq.sinajs.cn/list=hk00700,hk09988"


def test_parse_hq_lines_basic() -> None:
    text = (
        'var hq_str_hk00700="TENCENT,0,0,0";\n'
        'var hq_str_hk09988="ALIBABA,1,2";\n'
        "garbage line\n"
        'var hq_str_sh600000="not hk";\n'
    )
    out = sh.parse_hq_lines(text)
    assert out == [("00700", "TENCENT,0,0,0"), ("09988", "ALIBABA,1,2")]


def test_parse_hq_lines_empty_and_malformed() -> None:
    assert sh.parse_hq_lines("") == []
    assert sh.parse_hq_lines('var hq_str_hk00700="";') == [("00700", ";")]  # impl keeps trailing sep
    assert sh.parse_hq_lines("var hq_str_hkabc=\"x\"") == []
    assert sh.parse_hq_lines('var hq_str_hk00700="a') == [("00700", "a")]
