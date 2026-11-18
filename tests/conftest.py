from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rpa_control_center.api import create_app
from rpa_control_center.settings import Settings


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    app = create_app(Settings(tmp_path / "queue.db", api_token="test-token"))
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def headers() -> dict[str, str]:
    return {"X-API-Token": "test-token"}
