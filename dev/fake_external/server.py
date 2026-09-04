"""Programmable local substitutes for external LLM and IM APIs."""

from __future__ import annotations

import json
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock
from typing import Any

from dev.fake_external.scenarios import Scenario, plan_scenario

MESSAGES: list[dict[str, Any]] = []
SCENARIOS = {"llm": Scenario.SUCCESS, "im": Scenario.SUCCESS}
STATE_LOCK = Lock()


class FakeExternalHandler(BaseHTTPRequestHandler):
    server_version = "trpc-platform-fake/0.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler public API
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return

        if self.path == "/control/v1/scenarios":
            with STATE_LOCK:
                scenarios = {name: value.value for name, value in SCENARIOS.items()}
            self._json(HTTPStatus.OK, {"scenarios": scenarios})
            return

        if self.path == "/im/v1/messages":
            with STATE_LOCK:
                messages = list(MESSAGES)
            self._json(HTTPStatus.OK, {"messages": messages})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler public API
        payload = self._read_json()

        if self.path == "/control/v1/scenarios":
            self._configure_scenarios(payload)
            return

        if self.path == "/control/v1/reset":
            with STATE_LOCK:
                SCENARIOS.update(llm=Scenario.SUCCESS, im=Scenario.SUCCESS)
                MESSAGES.clear()
            self._json(HTTPStatus.OK, {"status": "reset"})
            return

        if self.path == "/llm/v1/chat/completions":
            self._serve_llm(payload)
            return

        if self.path == "/im/v1/messages":
            self._serve_im(payload)
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def log_message(self, format: str, *args: object) -> None:
        return

    def _configure_scenarios(self, payload: dict[str, Any]) -> None:
        try:
            requested = {
                name: Scenario(value)
                for name, value in payload.items()
                if name in SCENARIOS and isinstance(value, str)
            }
        except ValueError as error:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
            return

        if not requested or len(requested) != len(payload):
            self._json(HTTPStatus.BAD_REQUEST, {"error": "expected llm and/or im scenario"})
            return

        with STATE_LOCK:
            SCENARIOS.update(requested)
        self._json(
            HTTPStatus.OK,
            {"scenarios": {name: value.value for name, value in SCENARIOS.items()}},
        )

    def _serve_llm(self, payload: dict[str, Any]) -> None:
        plan = plan_scenario(SCENARIOS["llm"])
        if self._apply_transport_plan(plan.delay_seconds, plan.disconnect):
            return
        if plan.status != HTTPStatus.OK:
            self._json(plan.status, {"error": SCENARIOS["llm"].value})
            return
        if payload.get("stream"):
            self._serve_llm_stream(payload)
            return
        self._json(
            HTTPStatus.OK,
            {
                "id": "fake-completion-1",
                "object": "chat.completion",
                "model": payload.get("model", "fake-model"),
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "fake reply"},
                        "finish_reason": "stop",
                    }
                ],
                "scenario": SCENARIOS["llm"].value,
            },
        )

    def _serve_llm_stream(self, payload: dict[str, Any]) -> None:
        """Answer OpenAI chat completions with deterministic SSE deltas."""
        completion_id = "fake-completion-stream-1"
        model = payload.get("model", "fake-model")
        chunks = [
            {
                "id": completion_id,
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
            },
            {
                "id": completion_id,
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [
                    {"index": 0, "delta": {"content": "fake reply"}, "finish_reason": None}
                ],
            },
            {
                "id": completion_id,
                "object": "chat.completion.chunk",
                "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            },
        ]
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        for chunk in chunks:
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def _serve_im(self, payload: dict[str, Any]) -> None:
        plan = plan_scenario(SCENARIOS["im"])
        if self._apply_transport_plan(plan.delay_seconds, plan.disconnect):
            return
        if plan.status not in {HTTPStatus.OK, HTTPStatus.ACCEPTED}:
            self._json(plan.status, {"error": SCENARIOS["im"].value})
            return

        with STATE_LOCK:
            for _ in range(plan.delivery_count):
                if plan.prepend_delivery:
                    MESSAGES.insert(0, payload)
                else:
                    MESSAGES.append(payload)
            delivery_id = f"fake-{len(MESSAGES)}"
        self._json(
            plan.status,
            {"delivery_id": delivery_id, "scenario": SCENARIOS["im"].value},
        )

    def _apply_transport_plan(self, delay_seconds: float, disconnect: bool) -> bool:
        if delay_seconds:
            time.sleep(delay_seconds)
        if disconnect:
            self.close_connection = True
            return True
        return False

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        value: object = json.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    def _json(self, status: HTTPStatus, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", 8090), FakeExternalHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
