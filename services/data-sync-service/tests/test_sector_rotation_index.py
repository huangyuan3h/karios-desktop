from __future__ import annotations

from data_sync_service.service.sector_rotation_index import (
    SRV_LEVEL_ELEVATED,
    SRV_LEVEL_EXTREME_HIGH,
    SRV_LEVEL_STABLE,
    classify_srv_score,
    compute_srv_index,
)


def _fixture_top_by_date() -> list[dict]:
    return [
        {
            "date": "2026-06-16",
            "top": ["电子", "半导体", "通信", "AI", "银行"],
        },
        {
            "date": "2026-06-17",
            "top": ["电子", "半导体", "新能源", "医药", "军工"],
        },
        {
            "date": "2026-06-18",
            "top": ["电子", "半导体", "通信", "消费", "地产"],
        },
    ]


def test_classify_srv_score_matrix() -> None:
    assert classify_srv_score(30.0) == SRV_LEVEL_STABLE
    assert classify_srv_score(44.9) == SRV_LEVEL_STABLE
    assert classify_srv_score(45.0) == SRV_LEVEL_ELEVATED
    assert classify_srv_score(64.9) == SRV_LEVEL_ELEVATED
    assert classify_srv_score(65.0) == SRV_LEVEL_EXTREME_HIGH
    assert classify_srv_score(100.0) == SRV_LEVEL_EXTREME_HIGH


def test_compute_srv_index_mainline_fixture() -> None:
    # Stable leader (电子 x3) but unique=10 / pairwise=2 → score 45.5 → Elevated
    out = compute_srv_index(top_by_date=_fixture_top_by_date(), as_of_date="2026-06-18")
    assert out["level"] == SRV_LEVEL_ELEVATED
    assert out["overlapCount"] == 2
    assert set(out["overlapSectors"]) == {"电子", "半导体"}
    assert out["dates"] == ["2026-06-16", "2026-06-17", "2026-06-18"]
    assert out["score"] == 45.5


def test_compute_srv_index_elevated_overlap_2() -> None:
    top_by_date = [
        {"date": "2026-06-16", "top": ["A", "B", "C", "D", "E"]},
        {"date": "2026-06-17", "top": ["A", "B", "F", "G", "H"]},
        {"date": "2026-06-18", "top": ["A", "B", "I", "J", "K"]},
    ]
    out = compute_srv_index(top_by_date=top_by_date, as_of_date="2026-06-18")
    assert out["level"] == SRV_LEVEL_ELEVATED
    assert out["overlapCount"] == 2
    assert out["overlapSectors"] == ["A", "B"]
    assert 45.0 <= out["score"] < 65.0


def test_compute_srv_index_extreme_high_overlap_0() -> None:
    top_by_date = [
        {"date": "2026-06-16", "top": ["A", "B", "C", "D", "E"]},
        {"date": "2026-06-17", "top": ["F", "G", "H", "I", "J"]},
        {"date": "2026-06-18", "top": ["K", "L", "M", "N", "O"]},
    ]
    out = compute_srv_index(top_by_date=top_by_date, as_of_date="2026-06-18")
    assert out["level"] == SRV_LEVEL_EXTREME_HIGH
    assert out["overlapCount"] == 0
    assert out["overlapSectors"] == []
    assert out["score"] >= 65.0


def test_compute_srv_index_extreme_high_overlap_1() -> None:
    top_by_date = [
        {"date": "2026-06-16", "top": ["A", "B", "C", "D", "E"]},
        {"date": "2026-06-17", "top": ["A", "F", "G", "H", "I"]},
        {"date": "2026-06-18", "top": ["J", "K", "L", "M", "N"]},
    ]
    out = compute_srv_index(top_by_date=top_by_date, as_of_date="2026-06-18")
    assert out["level"] == SRV_LEVEL_EXTREME_HIGH
    assert out["overlapCount"] == 0


def test_compute_srv_index_stable_overlap_3_sectors() -> None:
    top_by_date = [
        {"date": "2026-06-16", "top": ["A", "B", "C", "D", "E"]},
        {"date": "2026-06-17", "top": ["A", "B", "C", "F", "G"]},
        {"date": "2026-06-18", "top": ["A", "B", "C", "H", "I"]},
    ]
    out = compute_srv_index(top_by_date=top_by_date, as_of_date="2026-06-18")
    assert out["level"] == SRV_LEVEL_STABLE
    assert out["overlapCount"] == 3
    assert out["overlapSectors"] == ["A", "B", "C"]


def test_compute_srv_index_insufficient_dates() -> None:
    top_by_date = [
        {"date": "2026-06-17", "top": ["A", "B", "C", "D", "E"]},
        {"date": "2026-06-18", "top": ["A", "B", "F", "G", "H"]},
    ]
    out = compute_srv_index(top_by_date=top_by_date, as_of_date="2026-06-18")
    assert out["level"] is None
    assert out["overlapCount"] is None
    assert out["dates"] == []
    assert out["score"] is None


def test_compute_srv_index_empty_top_on_day() -> None:
    top_by_date = [
        {"date": "2026-06-16", "top": ["A", "B", "C", "D", "E"]},
        {"date": "2026-06-17", "top": []},
        {"date": "2026-06-18", "top": ["A", "B", "F", "G", "H"]},
    ]
    out = compute_srv_index(top_by_date=top_by_date, as_of_date="2026-06-18")
    assert out["level"] is None
    assert out["overlapCount"] is None
