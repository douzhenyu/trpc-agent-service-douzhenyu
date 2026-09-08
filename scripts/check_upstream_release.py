"""Check PyPI for a stable trpc-agent-py upgrade candidate.

Used by the daily upstream-upgrade workflow. Prints the candidate version
and sets ``upgrade_available`` / ``candidate`` GitHub outputs; exits 0 in
both cases (the workflow branches on the output).
"""

from __future__ import annotations

import argparse
import json
import urllib.request

from trpc_service.upstream import latest_stable

PYPI_URL = "https://pypi.org/pypi/trpc-agent-py/json"


def fetch_releases(url: str = PYPI_URL) -> list[str]:
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        payload = json.load(response)
    return sorted(payload.get("releases", {}).keys())


def main() -> None:
    parser = argparse.ArgumentParser(description="Check for stable upstream upgrades.")
    parser.add_argument("--current", required=True, help="currently pinned version")
    parser.add_argument("--url", default=PYPI_URL)
    arguments = parser.parse_args()

    candidate = latest_stable(fetch_releases(arguments.url), arguments.current)
    if candidate is None:
        print("upgrade_available=false")
        return
    print("upgrade_available=true")
    print(f"candidate={candidate}")


if __name__ == "__main__":
    main()
