.PHONY: install lint format test check pre-commit-upgrade services-up services-down

install:
	uv sync

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest

check: lint test

pre-commit-upgrade:
	uv run pre-commit autoupdate

services-up:
	docker compose up -d

services-down:
	docker compose down
