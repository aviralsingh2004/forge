# Documentation

Project documentation and design notes belong here. The v0.1 specification
is kept in `Forge v0.1 Project Specification.md` as the initial source of truth.

Phase 1 uses UUID primary keys for every persisted entity. Database connection
settings are read from `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`,
`POSTGRES_HOST`, and `POSTGRES_PORT`; no connection details are hard-coded in
the application.
