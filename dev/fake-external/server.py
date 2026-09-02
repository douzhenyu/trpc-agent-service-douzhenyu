"""Deterministic local substitutes for external LLM and IM APIs."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

MESSAGES: list[dict[str, Any]] = []


class FakeExternalHandler(BaseHTTPRequestHandler):
    server_version = "trpc-platform-fake/0.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler public API
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return

        if self.path == "/im/v1/messages":
            self._json(HTTPStatus.OK, {"messages": MESSAGES})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler public API
        payload = self._read_json()

        if self.path == "/llm/v1/chat/completions":
            self._json(
                HTTPStatus.OK,
                {
                    "id": "fake-completion-1",
                    "model": payload.get("model", "fake-model"),
                    "choices": [
                        {"index": 0, "message": {"role": "assistant", "content": "fake reply"}}
                    ],
                },
            )
            return

        if self.path == "/im/v1/messages":
            MESSAGES.append(payload)
            self._json(HTTPStatus.ACCEPTED, {"delivery_id": f"fake-{len(MESSAGES)}"})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def log_message(self, format: str, *args: object) -> None:
        return

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
