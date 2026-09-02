"""Admin API process entry point."""

import uvicorn


def main() -> None:
    uvicorn.run(
        "trpc_service.admin_api.app:app",
        host="0.0.0.0",  # noqa: S104 - the container must accept cluster traffic
        port=8000,
    )


if __name__ == "__main__":
    main()
