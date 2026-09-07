# Forge

Forge is a learning-focused distributed job scheduling and execution platform.
The control plane uses PostgreSQL as its source of truth, Redis Streams as the
scheduling event mechanism, and push-based workers for execution.

## Phase 0

The repository bootstrap includes:

- Python 3.12+ project configuration managed with `uv`
- Ruff formatting and linting
- pytest test configuration
- pre-commit hooks
- Docker Compose services for PostgreSQL and Redis
- reserved directories for the backend, frontend, deployment files, and docs

Feature implementation starts in Phase 1 with the PostgreSQL models and
migrations. Kubernetes, RabbitMQ, Kafka, and adaptive scheduling are out of
scope for v0.1.

## Local setup

Install `uv`, Docker, and Docker Compose. Then run:

```text
uv sync
docker compose up -d
uv run pytest
uv run ruff check .
```

The same commands are available through the Makefile targets `install`,
`services-up`, `test`, `lint`, and `check`.

Copy `.env.example` to `.env` before changing local connection settings.

## Phase 1 database commands

With PostgreSQL running and the database variables available in the environment:

```text
uv run alembic upgrade head
uv run pytest
```

Phase 1 uses UUID primary keys consistently across all tables. PostgreSQL is
the authoritative store; Redis is not used by this phase.
