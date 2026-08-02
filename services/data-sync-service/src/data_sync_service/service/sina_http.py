"""Shared HTTP helper for Sina Finance quote APIs (hq.sinajs.cn).

Used for HK realtime quotes as a more accurate alternative to East Money push2.
Sina's HK feed is the same data source that powers most popular Chinese stock
apps (Tonghuashun, Xueqiu, etc.) for HK tickers, so prices line up with what
users see in those clients.

The endpoint returns GBK-encoded `var hq_str_*="..."` lines that need decoding.
A Referer is required or the server returns an HTML error page.
"""

from __future__ import annotations

import urllib.parse
import urllib.request

_SINA_REFERER = "https://finance.sina.com.cn/"


def _sina_headers(referer: str = _SINA_REFERER) -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "Connection": "keep-alive",
    }


def _requests_get(url: str, *, timeout: float) -> str:
    import requests  # type: ignore[import-not-found]

    resp = requests.get(
        url,
        headers=_sina_headers(),
        timeout=timeout,
        proxies={"http": None, "https": None},
    )
    if resp.status_code >= 400:
        preview = resp.text[:160]
        raise RuntimeError(f"http_{resp.status_code}:{preview}")
    # Sina's HK endpoint serves GBK / GB18030; resp.apparent_encoding is reliable.
    if not resp.encoding or resp.encoding.lower().replace("-", "") in {"utf8", "ascii"}:
        resp.encoding = "gb18030"
    return resp.text


def _urllib_get(url: str, *, timeout: float) -> str:
    req = urllib.request.Request(url, headers=_sina_headers())
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        status = getattr(resp, "status", 200)
    if int(status) >= 400:
        preview = raw[:160].decode("utf-8", errors="replace")
        raise RuntimeError(f"urllib_http_{status}:{preview}")
    # Sina HK is GBK; decode explicitly.
    return raw.decode("gb18030", errors="replace")


def sina_get_text(url: str, *, timeout: float = 10.0) -> str:
    """Fetch a Sina hq.sinajs.cn URL and return the decoded body text.

    Raises RuntimeError on network failure or non-2xx response.
    """
    errors: list[str] = []
    try:
        return _requests_get(url, timeout=timeout)
    except Exception as e:  # noqa: BLE001
        errors.append(f"requests:{e}")
    try:
        return _urllib_get(url, timeout=timeout)
    except Exception as e:  # noqa: BLE001
        errors.append(f"urllib:{e}")
    raise RuntimeError("; ".join(errors[-2:]))


def build_hq_url(tickers: list[str]) -> str:
    """Build hq.sinajs.cn/list=hk00700,hk09988,... URL for HK tickers."""
    encoded = [urllib.parse.quote(f"hk{t}", safe="") for t in tickers]
    return f"https://hq.sinajs.cn/list={','.join(encoded)}"


def parse_hq_lines(text: str) -> list[tuple[str, str]]:
    """Parse hq.sinajs.cn response body into [(ticker, payload), ...].

    Each line looks like: `var hq_str_hk00700="TENCENT,...,475.200,..."`.
    Returns the raw payload string for the caller to field-split.
    Empty / malformed lines are dropped.
    """
    out: list[tuple[str, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("var hq_str_"):
            continue
        head, _, rest = line.partition('="')
        if not rest:
            continue
        payload = rem = ""
        payload, _, rem = rest.rpartition('"')
        if not payload and rem:
            payload = rem
        if not payload:
            continue
        marker = head[len("var hq_str_") :]
        if not marker.startswith("hk"):
            continue
        ticker = marker[len("hk") :].strip()
        if not ticker or not ticker.isdigit():
            continue
        out.append((ticker, payload))
    return out