"""Controlled entry point for production database migrations.

The current platform skeleton has no schema revisions. Keeping this entry point in
the runtime image lets Argo CD gate every release on a migration Job; the database
phase can register Alembic revisions behind the same deployment boundary.
"""

from __future__ import annotations

import json


def main() -> None:
    """Report the empty migration set and complete the release gate."""
    print(json.dumps({"status": "ok", "applied_revisions": 0}))


if __name__ == "__main__":
    main()
