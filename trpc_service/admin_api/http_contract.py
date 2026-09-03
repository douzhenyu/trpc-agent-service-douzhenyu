"""Shared OpenAPI metadata for stable Admin API HTTP contracts."""

from typing import Any

from trpc_service.admin_api.schemas import ErrorResponse

ERROR_RESPONSES: dict[int, dict[str, Any]] = {
    400: {"model": ErrorResponse, "description": "Invalid request"},
    401: {"model": ErrorResponse, "description": "Authentication failed"},
    403: {"model": ErrorResponse, "description": "Authorization failed"},
    404: {"model": ErrorResponse, "description": "Resource not found"},
    409: {"model": ErrorResponse, "description": "Command conflict"},
    412: {"model": ErrorResponse, "description": "Version precondition failed"},
    422: {"model": ErrorResponse, "description": "Request validation failed"},
    500: {"model": ErrorResponse, "description": "Internal error"},
    502: {"model": ErrorResponse, "description": "Identity provider unavailable"},
}

ETAG_HEADER = {
    "ETag": {
        "description": "Quoted current resource version",
        "schema": {"type": "string"},
    }
}


def error_responses(*status_codes: int) -> dict[int | str, dict[str, Any]]:
    return {status_code: ERROR_RESPONSES[status_code] for status_code in status_codes}
