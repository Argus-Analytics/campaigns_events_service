FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN uv sync --no-dev

COPY src/ ./src/

ENV PYTHONPATH=/app/src

EXPOSE 8302

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8302"]
