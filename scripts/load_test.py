"""Repeatable load test for the capacity envelope (issue #34 acceptance).

Profiles:
  sustained — 1000 msg/s for 60 seconds
  burst     — 3000 msg/s for 60 seconds
  soak      — 200 msg/s for 300 seconds (steady-state baseline)

Run against a live stack, e.g.:
  uv run python scripts/load_test.py --url http://gateway.internal \\
      --tenant <uuid> --application <uuid> --profile burst

Exit code is non-zero when the profile's acceptance thresholds fail:
  accepted-request p99 latency <= 5s and accepted throughput >= 90% of the
  requested rate. Fast 429 responses are reported but never count as capacity.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from typing import Any
from uuid import uuid4

import httpx

PROFILES: dict[str, dict[str, int]] = {
    "sustained": {"rate": 1000, "seconds": 60},
    "burst": {"rate": 3000, "seconds": 60},
    "soak": {"rate": 200, "seconds": 300},
}

ACCEPTED_STATUSES = {200, 202}  # 429 rejections are shed, not errors
MAX_CLIENT_CONCURRENCY = 10_000


async def _send_one(
    client: httpx.AsyncClient, url: str, payload: dict[str, Any]
) -> tuple[float, int]:
    started = time.perf_counter()
    response = await client.post(url, json=payload)
    elapsed = time.perf_counter() - started
    if response.status_code not in ACCEPTED_STATUSES and response.status_code != 429:
        raise RuntimeError(f"unexpected status {response.status_code}: {response.text[:200]}")
    return elapsed, response.status_code


async def run(profile: str, url: str, tenant: str, application: str) -> dict[str, Any]:
    settings = PROFILES[profile]
    rate = settings["rate"]
    duration = settings["seconds"]
    interval = 1.0 / rate
    accepted_latencies: list[float] = []
    status_counts: dict[str, int] = {}

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        started = time.perf_counter()
        deadline = started + duration
        attempted = 0
        late_responses = 0
        concurrency = asyncio.Semaphore(MAX_CLIENT_CONCURRENCY)
        requests: set[asyncio.Task[None]] = set()

        async def record_request(payload: dict[str, Any]) -> None:
            nonlocal late_responses
            try:
                elapsed, status = await _send_one(client, url, payload)
                completed_at = time.perf_counter()
                key = f"{status // 100}xx/{status}"
                status_counts[key] = status_counts.get(key, 0) + 1
                if status in ACCEPTED_STATUSES and completed_at <= deadline:
                    accepted_latencies.append(elapsed)
                elif status in ACCEPTED_STATUSES:
                    late_responses += 1
            finally:
                concurrency.release()

        while True:
            offset = attempted * interval
            if offset >= duration:
                break
            payload = {
                "tenant_id": tenant,
                "application_id": application,
                "environment": "PRODUCTION",
                "session_id": f"load-{uuid4().hex[:8]}",
                "messages": [{"role": "user", "content": f"load-{attempted}"}],
                "message_id": f"load-{uuid4()}",
            }
            ahead = offset - (time.perf_counter() - started)
            if ahead > 0:
                await asyncio.sleep(ahead)
            if time.perf_counter() >= deadline:
                break
            await concurrency.acquire()
            request = asyncio.create_task(record_request(payload))
            requests.add(request)
            request.add_done_callback(requests.discard)
            attempted += 1

        if requests:
            await asyncio.gather(*requests)

    accepted = len(accepted_latencies)

    return {
        "profile": profile,
        "requested_rate": rate,
        "attempted": attempted,
        "accepted": accepted,
        "late_accepted": late_responses,
        "attempted_rate": round(attempted / duration, 1),
        "accepted_rate": round(accepted / duration, 1),
        "p50_seconds": (
            round(statistics.median(accepted_latencies), 4) if accepted_latencies else 0.0
        ),
        "p99_seconds": (
            round(statistics.quantiles(accepted_latencies, n=100)[98], 4)
            if len(accepted_latencies) > 1
            else 0.0
        ),
        "max_seconds": round(max(accepted_latencies), 4) if accepted_latencies else 0.0,
        "status_counts": status_counts,
    }


def evaluate(report: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if report["p99_seconds"] > 5.0:
        failures.append(f"p99 {report['p99_seconds']}s exceeds the 5s bound")
    if report["accepted_rate"] < report["requested_rate"] * 0.9:
        failures.append(
            f"accepted {report['accepted_rate']}/s below 90% of {report['requested_rate']}/s"
        )
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one capacity load profile.")
    parser.add_argument("--url", required=True, help="agent-gateway base URL")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--application", required=True)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="sustained")
    arguments = parser.parse_args()

    url = arguments.url.rstrip("/") + "/internal/v1/agent-executions"
    report = asyncio.run(run(arguments.profile, url, arguments.tenant, arguments.application))
    print(json.dumps(report, indent=2))
    failures = evaluate(report)
    for failure in failures:
        print(f"FAIL {failure}")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
