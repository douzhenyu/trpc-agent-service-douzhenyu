"""Sandbox image entrypoint contract.

The sandbox image reads the JSON payload from SANDBOX_PAYLOAD_B64 (base64 of
{"code": str, "inputs": [{"name", "content(base64)"}]}), writes the decoded
input artifacts to a temporary directory, executes the code with the same
interpreter and prints the result to stdout. Everything printed becomes the
execution output returned to the Agent; nothing else leaves the pod.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import tempfile
from pathlib import Path


def main() -> int:
    payload_b64 = os.environ.get("SANDBOX_PAYLOAD_B64", "")
    if not payload_b64:
        print("missing SANDBOX_PAYLOAD_B64", file=sys.stderr)
        return 2
    payload = json.loads(base64.b64decode(payload_b64))
    workdir = Path(tempfile.mkdtemp(prefix="sandbox-"))
    for artifact in payload.get("inputs", []):
        target = workdir / artifact["name"]
        target.write_bytes(base64.b64decode(artifact["content"]))
    os.chdir(workdir)
    code = payload.get("code", "")
    try:
        exec(compile(code, "<sandbox>", "exec"), {"__name__": "__main__"})  # noqa: S102
    except SystemExit as exit_request:  # code may exit explicitly
        return int(exit_request.code or 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
