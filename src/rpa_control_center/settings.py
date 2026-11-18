from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Settings:
    database_path: Path
    api_token: str | None = None

    @classmethod
    def from_env(cls) -> Settings:
        import os

        return cls(
            database_path=Path(os.getenv("RPA_DATABASE_PATH", "rpa-control-center.db")),
            api_token=os.getenv("RPA_API_TOKEN") or None,
        )
