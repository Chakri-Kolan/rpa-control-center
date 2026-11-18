from __future__ import annotations

import json
from pathlib import Path

from rpa_control_center.api import create_app


def main() -> None:
    destination = Path("integrations/openapi.json")
    destination.write_text(json.dumps(create_app().openapi(), indent=2) + "\n", encoding="utf-8")
    print(destination)


if __name__ == "__main__":
    main()
