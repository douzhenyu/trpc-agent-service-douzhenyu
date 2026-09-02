FROM python:3.12.13-slim

WORKDIR /app
COPY server.py ./server.py

EXPOSE 8090

USER 65532:65532

CMD ["python", "server.py"]
