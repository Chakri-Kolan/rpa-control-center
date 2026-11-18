from __future__ import annotations

from fastapi.testclient import TestClient


def enqueue(client: TestClient, headers: dict[str, str], reference: str = "SO-1001"):
    return client.post(
        "/api/v1/items",
        headers=headers,
        json={
            "queue_name": "sap-orders",
            "reference": reference,
            "payload": {"order_id": reference},
            "priority": 10,
            "max_attempts": 2,
            "actor": "sap-intake",
        },
    )


def test_health_is_public(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["X-Request-ID"]


def test_api_requires_token(client: TestClient) -> None:
    response = client.get("/api/v1/metrics")
    assert response.status_code == 401


def test_enqueue_is_idempotent(client: TestClient, headers: dict[str, str]) -> None:
    first = enqueue(client, headers)
    second = enqueue(client, headers)
    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["item"]["id"] == second.json()["item"]["id"]
    assert second.json()["created"] is False


def test_lease_complete_and_audit(client: TestClient, headers: dict[str, str]) -> None:
    item_id = enqueue(client, headers).json()["item"]["id"]
    leased = client.post(
        "/api/v1/queues/sap-orders/lease",
        headers=headers,
        json={"robot_id": "uipath-robot-01", "lease_seconds": 60},
    )
    assert leased.status_code == 200
    assert leased.json()["item"]["attempts"] == 1
    completed = client.post(
        f"/api/v1/items/{item_id}/complete",
        headers=headers,
        json={"robot_id": "uipath-robot-01", "result": {"document": "posted"}},
    )
    assert completed.status_code == 200
    assert completed.json()["item"]["status"] == "completed"
    events = client.get(f"/api/v1/items/{item_id}/audit", headers=headers).json()["events"]
    assert [event["event_type"] for event in events] == ["enqueued", "leased", "completed"]


def test_retry_then_dead_letter(client: TestClient, headers: dict[str, str]) -> None:
    item_id = enqueue(client, headers, "SO-2002").json()["item"]["id"]
    for expected_status in ("pending", "dead"):
        client.post(
            "/api/v1/queues/sap-orders/lease",
            headers=headers,
            json={"robot_id": "pad-robot-01", "lease_seconds": 60},
        )
        failed = client.post(
            f"/api/v1/items/{item_id}/fail",
            headers=headers,
            json={
                "robot_id": "pad-robot-01",
                "error": "  SAP   timeout  ",
                "retry_delay_seconds": 0,
            },
        )
        assert failed.json()["item"]["status"] == expected_status
        assert failed.json()["item"]["last_error"] == "SAP timeout"


def test_wrong_robot_cannot_finish(client: TestClient, headers: dict[str, str]) -> None:
    item_id = enqueue(client, headers, "SO-3003").json()["item"]["id"]
    client.post(
        "/api/v1/queues/sap-orders/lease",
        headers=headers,
        json={"robot_id": "robot-a", "lease_seconds": 60},
    )
    response = client.post(
        f"/api/v1/items/{item_id}/complete",
        headers=headers,
        json={"robot_id": "robot-b", "result": {}},
    )
    assert response.status_code == 409


def test_metrics_report_terminal_success(client: TestClient, headers: dict[str, str]) -> None:
    item_id = enqueue(client, headers, "SO-4004").json()["item"]["id"]
    client.post(
        "/api/v1/queues/sap-orders/lease",
        headers=headers,
        json={"robot_id": "robot-a", "lease_seconds": 60},
    )
    client.post(
        f"/api/v1/items/{item_id}/complete",
        headers=headers,
        json={"robot_id": "robot-a", "result": {}},
    )
    metrics = client.get("/api/v1/metrics", headers=headers).json()
    assert metrics["counts"]["completed"] == 1
    assert metrics["terminal_success_rate_percent"] == 100.0


def test_empty_queue_returns_no_content(client: TestClient, headers: dict[str, str]) -> None:
    response = client.post(
        "/api/v1/queues/empty/lease",
        headers=headers,
        json={"robot_id": "robot-a", "lease_seconds": 60},
    )
    assert response.status_code == 204
