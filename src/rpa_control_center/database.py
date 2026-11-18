from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


class QueueStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self._schema_lock = threading.Lock()
        self._initialized = False

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._schema_lock:
            if self._initialized:
                return
            with self.connection() as connection:
                connection.executescript(
                    """
                    PRAGMA journal_mode = WAL;
                    PRAGMA foreign_keys = ON;
                    CREATE TABLE IF NOT EXISTS work_items (
                        id TEXT PRIMARY KEY,
                        queue_name TEXT NOT NULL,
                        reference TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        priority INTEGER NOT NULL DEFAULT 0,
                        status TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        max_attempts INTEGER NOT NULL,
                        available_at TEXT NOT NULL,
                        lease_until TEXT,
                        robot_id TEXT,
                        result_json TEXT,
                        last_error TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        UNIQUE(queue_name, reference)
                    );
                    CREATE INDEX IF NOT EXISTS idx_work_items_lease
                        ON work_items(queue_name, status, available_at, priority DESC, created_at);
                    CREATE TABLE IF NOT EXISTS audit_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        item_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        detail_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY(item_id) REFERENCES work_items(id)
                    );
                    """
                )
            self._initialized = True

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.database_path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
        finally:
            connection.close()

    def enqueue(
        self,
        *,
        queue_name: str,
        reference: str,
        payload: dict[str, Any],
        priority: int,
        max_attempts: int,
        actor: str,
    ) -> tuple[dict[str, Any], bool]:
        self.initialize()
        now = as_timestamp(utc_now())
        item_id = str(uuid.uuid4())
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM work_items WHERE queue_name = ? AND reference = ?",
                (queue_name, reference),
            ).fetchone()
            if existing:
                connection.commit()
                return self._item(existing), False
            connection.execute(
                """
                INSERT INTO work_items (
                    id, queue_name, reference, payload_json, priority, status, attempts,
                    max_attempts, available_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    queue_name,
                    reference,
                    payload_json,
                    priority,
                    max_attempts,
                    now,
                    now,
                    now,
                ),
            )
            self._audit(connection, item_id, "enqueued", actor, {"queue": queue_name})
            row = connection.execute("SELECT * FROM work_items WHERE id = ?", (item_id,)).fetchone()
            connection.commit()
            return self._item(row), True

    def lease(self, *, queue_name: str, robot_id: str, lease_seconds: int) -> dict[str, Any] | None:
        self.initialize()
        now_value = utc_now()
        now = as_timestamp(now_value)
        lease_until = as_timestamp(now_value + timedelta(seconds=lease_seconds))
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE work_items
                   SET status = 'pending', lease_until = NULL, robot_id = NULL, updated_at = ?
                 WHERE status = 'leased' AND lease_until <= ? AND attempts < max_attempts
                """,
                (now, now),
            )
            connection.execute(
                """
                UPDATE work_items
                   SET status = 'dead', lease_until = NULL, robot_id = NULL, updated_at = ?
                 WHERE status = 'leased' AND lease_until <= ? AND attempts >= max_attempts
                """,
                (now, now),
            )
            row = connection.execute(
                """
                SELECT * FROM work_items
                 WHERE queue_name = ? AND status = 'pending' AND available_at <= ?
                   AND attempts < max_attempts
                 ORDER BY priority DESC, created_at ASC
                 LIMIT 1
                """,
                (queue_name, now),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            connection.execute(
                """
                UPDATE work_items
                   SET status = 'leased', attempts = attempts + 1, robot_id = ?,
                       lease_until = ?, updated_at = ?
                 WHERE id = ?
                """,
                (robot_id, lease_until, now, row["id"]),
            )
            self._audit(
                connection,
                row["id"],
                "leased",
                robot_id,
                {"lease_until": lease_until},
            )
            leased = connection.execute(
                "SELECT * FROM work_items WHERE id = ?", (row["id"],)
            ).fetchone()
            connection.commit()
            return self._item(leased)

    def complete(self, *, item_id: str, robot_id: str, result: dict[str, Any]) -> dict[str, Any]:
        return self._finish(
            item_id=item_id,
            robot_id=robot_id,
            result=result,
            error=None,
            retry_delay_seconds=0,
        )

    def fail(
        self,
        *,
        item_id: str,
        robot_id: str,
        error: str,
        retry_delay_seconds: int,
    ) -> dict[str, Any]:
        return self._finish(
            item_id=item_id,
            robot_id=robot_id,
            result=None,
            error=error,
            retry_delay_seconds=retry_delay_seconds,
        )

    def _finish(
        self,
        *,
        item_id: str,
        robot_id: str,
        result: dict[str, Any] | None,
        error: str | None,
        retry_delay_seconds: int,
    ) -> dict[str, Any]:
        self.initialize()
        now_value = utc_now()
        now = as_timestamp(now_value)
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM work_items WHERE id = ?", (item_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise LookupError("work item not found")
            if row["status"] != "leased" or row["robot_id"] != robot_id:
                connection.rollback()
                raise PermissionError("work item is not leased by this robot")
            if error is None:
                status = "completed"
                available_at = row["available_at"]
                event = "completed"
            else:
                status = "dead" if row["attempts"] >= row["max_attempts"] else "pending"
                available_at = as_timestamp(now_value + timedelta(seconds=retry_delay_seconds))
                event = "dead_lettered" if status == "dead" else "retry_scheduled"
            connection.execute(
                """
                UPDATE work_items
                   SET status = ?, available_at = ?, lease_until = NULL, robot_id = NULL,
                       result_json = ?, last_error = ?, updated_at = ?
                 WHERE id = ?
                """,
                (
                    status,
                    available_at,
                    json.dumps(result, sort_keys=True) if result is not None else None,
                    error,
                    now,
                    item_id,
                ),
            )
            self._audit(connection, item_id, event, robot_id, {"error": error})
            updated = connection.execute(
                "SELECT * FROM work_items WHERE id = ?", (item_id,)
            ).fetchone()
            connection.commit()
            return self._item(updated)

    def get(self, item_id: str) -> dict[str, Any] | None:
        self.initialize()
        with self.connection() as connection:
            row = connection.execute("SELECT * FROM work_items WHERE id = ?", (item_id,)).fetchone()
            return self._item(row) if row else None

    def audit(self, item_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM audit_events WHERE item_id = ? ORDER BY id", (item_id,)
            ).fetchall()
            return [
                {
                    "event_type": row["event_type"],
                    "actor": row["actor"],
                    "detail": json.loads(row["detail_json"]),
                    "created_at": row["created_at"],
                }
                for row in rows
            ]

    def metrics(self) -> dict[str, Any]:
        self.initialize()
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS count FROM work_items GROUP BY status"
            ).fetchall()
            counts = {row["status"]: row["count"] for row in rows}
            total_terminal = counts.get("completed", 0) + counts.get("dead", 0)
            success_rate = (
                round(counts.get("completed", 0) / total_terminal * 100, 2)
                if total_terminal
                else None
            )
            return {"counts": counts, "terminal_success_rate_percent": success_rate}

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        item_id: str,
        event_type: str,
        actor: str,
        detail: dict[str, Any],
    ) -> None:
        connection.execute(
            """
            INSERT INTO audit_events(item_id, event_type, actor, detail_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                item_id,
                event_type,
                actor,
                json.dumps(detail, sort_keys=True, separators=(",", ":")),
                as_timestamp(utc_now()),
            ),
        )

    @staticmethod
    def _item(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "queue_name": row["queue_name"],
            "reference": row["reference"],
            "payload": json.loads(row["payload_json"]),
            "priority": row["priority"],
            "status": row["status"],
            "attempts": row["attempts"],
            "max_attempts": row["max_attempts"],
            "available_at": row["available_at"],
            "lease_until": row["lease_until"],
            "robot_id": row["robot_id"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "last_error": row["last_error"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
