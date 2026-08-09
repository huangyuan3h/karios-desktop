"""tv_chrome: settings getters/setters, cdp version, profile copy, start/stop."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.service import tv_chrome as tvc


def test_setting_roundtrip(monkeypatch) -> None:
    calls = []

    def fake_set_value(key, value, updated_at):
        calls.append((key, value))

    monkeypatch.setattr(tvc.kv, "set_value", fake_set_value)
    tvc.set_setting("k", "v")
    assert calls == [("k", "v")]


def test_pid_getters(monkeypatch) -> None:
    monkeypatch.setattr(tvc, "get_setting", lambda k: "12345")
    assert tvc._get_tv_chrome_pid() == 12345
    monkeypatch.setattr(tvc, "get_setting", lambda k: "  ")
    assert tvc._get_tv_chrome_pid() is None
    monkeypatch.setattr(tvc, "get_setting", lambda k: "abc")
    assert tvc._get_tv_chrome_pid() is None

    saved = {}
    monkeypatch.setattr(tvc, "set_setting", lambda k, v: saved.__setitem__(k, v))
    tvc._set_tv_chrome_pid(99)
    assert saved["tv_chrome_pid"] == "99"
    tvc._set_tv_chrome_pid(None)
    assert saved["tv_chrome_pid"] == ""


def test_cdp_port_getter(monkeypatch) -> None:
    monkeypatch.setattr(tvc, "get_setting", lambda k: "9223")
    assert tvc._get_tv_cdp_port() == 9223
    monkeypatch.setattr(tvc, "get_setting", lambda k: "x")
    assert tvc._get_tv_cdp_port() == tvc.TV_CDP_PORT_DEFAULT
    monkeypatch.setattr(tvc, "get_setting", lambda k: "")
    assert tvc._get_tv_cdp_port() == tvc.TV_CDP_PORT_DEFAULT


def test_headless_and_bin_getters(monkeypatch) -> None:
    monkeypatch.setattr(tvc, "get_setting", lambda k: "")
    assert tvc._get_tv_headless() is True
    monkeypatch.setattr(tvc, "get_setting", lambda k: "on")
    assert tvc._get_tv_headless() is True
    monkeypatch.setattr(tvc, "get_setting", lambda k: "0")
    assert tvc._get_tv_headless() is False
    monkeypatch.setattr(tvc, "get_setting", lambda k: "  /usr/bin/chrome  ")
    assert tvc._get_tv_chrome_bin() == "/usr/bin/chrome"
    monkeypatch.setattr(tvc, "get_setting", lambda k: "")
    assert tvc._get_tv_chrome_bin() == tvc.TV_CHROME_BIN_DEFAULT


def test_tcp_is_listening(monkeypatch) -> None:
    class _FakeSocket:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    import socket as _socket

    monkeypatch.setattr(_socket, "create_connection", lambda addr, timeout: _FakeSocket())
    assert tvc._tcp_is_listening("127.0.0.1", 9222) is True
    monkeypatch.setattr(_socket, "create_connection", lambda addr, timeout: (_ for _ in ()).throw(OSError("refused")))
    assert tvc._tcp_is_listening("127.0.0.1", 9222) is False


class _HttpResp:
    def __init__(self, body):
        self._b = body.encode() if isinstance(body, str) else body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return self._b


def test_cdp_version(monkeypatch) -> None:
    monkeypatch.setattr(tvc.urllib.request, "urlopen", lambda url, timeout=0.8: _HttpResp('{"Browser": "Chrome/126", "webSocketDebuggerUrl": "ws://x"}'))
    out = tvc._cdp_version("127.0.0.1", 9222)
    assert out["Browser"] == "Chrome/126"

    monkeypatch.setattr(tvc.urllib.request, "urlopen", lambda url, timeout=0.8: (_ for _ in ()).throw(OSError("conn refused")))
    assert tvc._cdp_version("127.0.0.1", 9222) is None

    monkeypatch.setattr(tvc.urllib.request, "urlopen", lambda url, timeout=0.8: _HttpResp("not json"))
    assert tvc._cdp_version("127.0.0.1", 9222) is None


def test_copy_chrome_profile(monkeypatch, tmp_path) -> None:
    src_ud = tmp_path / "src-ud"
    dst_ud = tmp_path / "dst-ud"
    src_profile = src_ud / "Profile 1"
    src_profile.mkdir(parents=True)
    (src_ud / "Local State").write_text("state")
    (src_profile / "Cookies").write_text("c")
    (src_profile / "Cache").mkdir()
    (src_profile / "Cache" / "x").write_text("cache")

    monkeypatch.setattr(tvc, "_home_path", lambda p: p)
    tvc._copy_chrome_profile(
        src_user_data_dir=str(src_ud), src_profile_dir="Profile 1",
        dst_user_data_dir=str(dst_ud), dst_profile_dir="Profile 1",
        force=False,
    )
    assert (dst_ud / "Local State").exists()
    assert (dst_ud / "Profile 1" / "Cookies").exists()
    assert not (dst_ud / "Profile 1" / "Cache").exists()  # skipped cache

    with pytest.raises(HTTPException) as e1:
        tvc._copy_chrome_profile(
            src_user_data_dir=str(tmp_path / "nope"), src_profile_dir="P",
            dst_user_data_dir=str(dst_ud), dst_profile_dir="P", force=False,
        )
    assert e1.value.status_code == 400

    with pytest.raises(HTTPException) as e2:
        tvc._copy_chrome_profile(
            src_user_data_dir=str(src_ud), src_profile_dir="NoSuch",
            dst_user_data_dir=str(dst_ud), dst_profile_dir="P", force=False,
        )
    assert e2.value.status_code == 400

    # existing dest without force → no-op
    (dst_ud / "Profile 1" / "Cookies").write_text("modified")
    tvc._copy_chrome_profile(
        src_user_data_dir=str(src_ud), src_profile_dir="Profile 1",
        dst_user_data_dir=str(dst_ud), dst_profile_dir="Profile 1",
        force=False,
    )
    assert (dst_ud / "Profile 1" / "Cookies").read_text() == "modified"
    # force → re-copy
    tvc._copy_chrome_profile(
        src_user_data_dir=str(src_ud), src_profile_dir="Profile 1",
        dst_user_data_dir=str(dst_ud), dst_profile_dir="Profile 1",
        force=True,
    )
    assert (dst_ud / "Profile 1" / "Cookies").read_text() == "c"


def test_status(monkeypatch) -> None:
    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: 999)
    monkeypatch.setattr(tvc, "_pid_is_running", lambda pid: True)
    monkeypatch.setattr(tvc, "_get_tv_cdp_port", lambda: 9222)
    monkeypatch.setattr(tvc, "_get_tv_user_data_dir", lambda: "/tmp/ud")
    monkeypatch.setattr(tvc, "_get_tv_profile_dir", lambda: "Profile 1")
    monkeypatch.setattr(tvc, "_get_tv_headless", lambda: True)
    monkeypatch.setattr(tvc, "_cdp_version", lambda host, port: {"Browser": "x"})
    st = tvc.status()
    assert st.running is True and st.cdpOk is True and st.port == 9222

    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: None)
    monkeypatch.setattr(tvc, "_cdp_version", lambda host, port: None)
    st2 = tvc.status()
    assert st2.running is False and st2.cdpOk is False


def test_start_already_running_same_config(monkeypatch) -> None:
    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: 999)
    monkeypatch.setattr(tvc, "_pid_is_running", lambda pid: True)
    monkeypatch.setattr(tvc, "_get_tv_cdp_port", lambda: 9222)
    monkeypatch.setattr(tvc, "_get_tv_user_data_dir", lambda: "/tmp/ud")
    monkeypatch.setattr(tvc, "_get_tv_profile_dir", lambda: "Profile 1")
    monkeypatch.setattr(tvc, "_get_tv_chrome_bin", lambda: "/usr/bin/chrome")
    monkeypatch.setattr(tvc, "_get_tv_headless", lambda: False)
    monkeypatch.setattr(tvc, "status", lambda: tvc.TvChromeStatus(running=True, pid=999, host="127.0.0.1", port=9222, cdpOk=True, cdpVersion={}, userDataDir="x", profileDirectory="y", headless=False))
    out = tvc._start_unlocked(port=9222, userDataDir="/tmp/ud", profileDirectory="Profile 1", chromeBin="/usr/bin/chrome", headless=False)
    assert out.running is True


def test_start_port_in_use_and_missing_bin(monkeypatch) -> None:
    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: None)
    monkeypatch.setattr(tvc, "_get_tv_cdp_port", lambda: 9222)
    monkeypatch.setattr(tvc, "_get_tv_user_data_dir", lambda: "/tmp/ud")
    monkeypatch.setattr(tvc, "_get_tv_profile_dir", lambda: "P")
    monkeypatch.setattr(tvc, "_get_tv_chrome_bin", lambda: "/usr/bin/chrome")
    monkeypatch.setattr(tvc, "_get_tv_headless", lambda: False)
    monkeypatch.setattr(tvc, "_tcp_is_listening", lambda host, port: True)
    with pytest.raises(HTTPException) as e:
        tvc._start_unlocked(port=9222, userDataDir="/tmp/ud", profileDirectory="P", chromeBin="/usr/bin/chrome", headless=False)
    assert e.value.status_code == 409

    monkeypatch.setattr(tvc, "_tcp_is_listening", lambda host, port: False)
    with pytest.raises(HTTPException) as e2:
        tvc._start_unlocked(port=9222, userDataDir="/tmp/ud", profileDirectory="P", chromeBin="/nonexistent/chrome", headless=False)
    assert e2.value.status_code == 400


def test_start_launches_and_stops(monkeypatch, tmp_path) -> None:
    saved = {}
    chrome_bin = tmp_path / "chrome-bin"
    chrome_bin.write_text("")

    class _Proc:
        pid = 4242

    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: None)
    monkeypatch.setattr(tvc, "_get_tv_cdp_port", lambda: 9222)
    monkeypatch.setattr(tvc, "_get_tv_user_data_dir", lambda: "/tmp/ud")
    monkeypatch.setattr(tvc, "_get_tv_profile_dir", lambda: "P")
    monkeypatch.setattr(tvc, "_get_tv_chrome_bin", lambda: str(chrome_bin))
    monkeypatch.setattr(tvc, "_get_tv_headless", lambda: False)
    monkeypatch.setattr(tvc, "_tcp_is_listening", lambda host, port: False)
    monkeypatch.setattr(tvc, "set_setting", lambda k, v: saved.__setitem__(k, v))
    monkeypatch.setattr(tvc, "_copy_chrome_profile", lambda **kw: None)
    monkeypatch.setattr(tvc, "subprocess", type("S", (), {"Popen": staticmethod(lambda args, **kw: _Proc()), "DEVNULL": -3})())
    monkeypatch.setattr(tvc, "_cdp_version", lambda host, port: {"Browser": "x"})
    monkeypatch.setattr(tvc, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    monkeypatch.setattr(tvc, "_home_path", lambda p: p)
    monkeypatch.setattr(tvc, "status", lambda: tvc.TvChromeStatus(running=True, pid=4242, host="127.0.0.1", port=9222, cdpOk=True, cdpVersion={}, userDataDir="ud", profileDirectory="P", headless=True))

    out = tvc._start_unlocked(
        port=9222, userDataDir=str(tmp_path / "ud"), profileDirectory="P",
        chromeBin=str(chrome_bin), headless=True,
        bootstrapFromChromeUserDataDir="/src/ud", bootstrapFromProfileDirectory="Profile 1",
        forceBootstrap=True,
    )
    assert out.running is True
    assert saved["tv_chrome_pid"] == "4242"
    assert saved["tv_bootstrap_src_user_data_dir"] == "/src/ud"
    assert saved["tv_bootstrap_src_profile_dir"] == "Profile 1"


def test_start_popen_failure(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: None)
    monkeypatch.setattr(tvc, "_get_tv_cdp_port", lambda: 9222)
    monkeypatch.setattr(tvc, "_get_tv_user_data_dir", lambda: "/tmp/ud")
    monkeypatch.setattr(tvc, "_get_tv_profile_dir", lambda: "P")
    monkeypatch.setattr(tvc, "_get_tv_chrome_bin", lambda: "/usr/bin/chrome")
    monkeypatch.setattr(tvc, "_get_tv_headless", lambda: False)
    monkeypatch.setattr(tvc, "_tcp_is_listening", lambda host, port: False)
    monkeypatch.setattr(tvc, "set_setting", lambda k, v: None)
    monkeypatch.setattr(tvc, "_home_path", lambda p: p)
    chrome_bin = tmp_path / "chrome-bin"
    chrome_bin.write_text("")

    def popen_fail(args, **kw):
        raise OSError("no chrome")

    monkeypatch.setattr(tvc.subprocess, "Popen", popen_fail)
    with pytest.raises(HTTPException) as e:
        tvc._start_unlocked(port=9222, userDataDir=str(tmp_path / "ud"), profileDirectory="P", chromeBin=str(chrome_bin), headless=False)
    assert e.value.status_code == 500


def test_stop_paths(monkeypatch) -> None:
    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: None)
    monkeypatch.setattr(tvc, "status", lambda: tvc.TvChromeStatus(running=False, pid=None, host="h", port=0, cdpOk=False, cdpVersion=None, userDataDir="u", profileDirectory="p", headless=False))
    assert tvc.stop().running is False

    monkeypatch.setattr(tvc, "_get_tv_chrome_pid", lambda: 777)
    monkeypatch.setattr(tvc, "_get_tv_cdp_port", lambda: 9222)
    monkeypatch.setattr(tvc, "_pid_is_running", lambda pid: False)
    monkeypatch.setattr(tvc, "_set_tv_chrome_pid", lambda pid: None)
    assert tvc.stop().running is False

    saved = {}
    monkeypatch.setattr(tvc, "_pid_is_running", lambda pid: True)
    monkeypatch.setattr(tvc, "os", type("O", (), {
        "killpg": staticmethod(lambda pid, sig: (_ for _ in ()).throw(OSError("no group"))),
        "kill": staticmethod(lambda pid, sig: None),
    })())
    monkeypatch.setattr(tvc, "time", type("T", (), {"sleep": staticmethod(lambda s: None)})())
    monkeypatch.setattr(tvc, "_tcp_is_listening", lambda host, port: False)
    monkeypatch.setattr(tvc, "_set_tv_chrome_pid", lambda pid: saved.__setitem__("cleared", pid))
    tvc.stop()
    assert saved["cleared"] is None

    monkeypatch.setattr(tvc, "_pid_is_running", lambda pid: True)
    monkeypatch.setattr(tvc, "os", type("O", (), {
        "killpg": staticmethod(lambda pid, sig: None),
        "kill": staticmethod(lambda pid, sig: None),
    })())
    tvc.stop()
    assert saved["cleared"] is None
