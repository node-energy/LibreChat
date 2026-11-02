FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY librechat_purge.py .

RUN uv pip install --system pymongo

ENTRYPOINT ["python", "librechat_purge.py"]
