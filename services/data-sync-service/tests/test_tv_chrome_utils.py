import pytest
from pathlib import Path
from data_sync_service.service.tv_chrome import (
    _now_iso,
    _home_path,
    _pid_is_running,
    _tcp_is_listening,
)


def test_now_iso_format():
    result = _now_iso()
    assert "T" in result


def test_home_path_with_tilde():
    result = _home_path("~")
    assert result != "~"
    assert Path(result).is_absolute()


def test_home_path_absolute():
    result = _home_path("/tmp/test")
    assert result == "/tmp/test"


def test_pid_is_running_current_process():
    import os
    current_pid = os.getpid()
    assert _pid_is_running(current_pid) is True


def test_pid_is_running_nonexistent():
    assert _pid_is_running(999999) is False


def test_tcp_is_listening_not_listening():
    assert _tcp_is_listening("127.0.0.1", 59999) is False