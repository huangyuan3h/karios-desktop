from fastapi.testclient import TestClient

from data_sync_service.main import app
from data_sync_service.scheduler import foo_job


def test_foo_endpoint() -> None:
    client = TestClient(app)
    response = client.get("/foo")
    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "foo ok"
    assert "timestamp" in payload


def test_foo_scheduler_status() -> None:
    client = TestClient(app)
    response = client.get("/scheduler/foo")
    assert response.status_code == 200
    payload = response.json()
    assert "trigger" in payload


def test_foo_job_writes_log(tmp_path, monkeypatch) -> None:
    log_path = tmp_path / "foo_job.log"
    monkeypatch.setattr(foo_job, "LOG_PATH", log_path)
    foo_job.run()

    content = log_path.read_text(encoding="utf-8")
    assert "foo ok" in content
