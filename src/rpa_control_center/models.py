from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class EnqueueRequest(BaseModel):
    queue_name: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_.-]+$")
    reference: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any]
    priority: int = Field(default=0, ge=-100, le=100)
    max_attempts: int = Field(default=3, ge=1, le=20)
    actor: str = Field(default="api", min_length=1, max_length=100)


class LeaseRequest(BaseModel):
    robot_id: str = Field(min_length=1, max_length=100)
    lease_seconds: int = Field(default=300, ge=10, le=3600)


class CompleteRequest(BaseModel):
    robot_id: str = Field(min_length=1, max_length=100)
    result: dict[str, Any] = Field(default_factory=dict)


class FailRequest(BaseModel):
    robot_id: str = Field(min_length=1, max_length=100)
    error: str = Field(min_length=1, max_length=2000)
    retry_delay_seconds: int = Field(default=60, ge=0, le=86400)

    @field_validator("error")
    @classmethod
    def normalize_error(cls, value: str) -> str:
        return " ".join(value.split())
