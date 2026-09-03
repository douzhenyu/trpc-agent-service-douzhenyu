FROM python:3.12.13-slim

WORKDIR /app
COPY dev/__init__.py ./dev/__init__.py
COPY dev/fake_external ./dev/fake_external

EXPOSE 8090

USER 65532:65532

CMD ["python", "-m", "dev.fake_external.server"]
