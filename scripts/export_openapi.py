"""Export the Admin API contract used to generate the Web Console client."""

import json
from pathlib import Path

from trpc_service.admin_api.app import app


def main() -> None:
    output = Path(__file__).parents[1] / "docs" / "contracts" / "admin-api.openapi.json"
    output.write_text(
        json.dumps(app.openapi(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
