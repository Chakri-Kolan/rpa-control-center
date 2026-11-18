from __future__ import annotations

from pathlib import Path

import pytest

from rpa_control_center.database import QueueStore


def test_get_missing_item(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "queue.db")
    assert store.get("missing") is None
    assert store.audit("missing") == []


def test_complete_missing_item(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "queue.db")
    with pytest.raises(LookupError):
        store.complete(item_id="missing", robot_id="robot", result={})
