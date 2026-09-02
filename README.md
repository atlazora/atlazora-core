# Atlazora Core

Atlazora Core is the Go transactional Core for Atlazora.

The repository follows the approved modular-first architecture. Transactional domains begin as explicit modules inside one Go Core rather than independent microservices or repositories.

## Architecture

The Core preserves explicit ownership boundaries even when modules share the same runtime and PostgreSQL infrastructure.

Approved transactional domain boundaries:

- Identity
- Supplier
- Catalog
- Sourcing
- Commerce
- Finance
- Logistics
- Inspection
- Disputes
- Trust
- Growth
- Platform

Every authoritative transactional data type has one owning domain.

PostgreSQL is the authoritative transactional datastore. Redis, search indexes, analytics projections, and intelligence outputs are ephemeral or derived and are not transactional truth.

Shared OpenAPI, event, and platform contracts belong in atlazora-contracts and are outside W00-WU03. Transactional Outbox implementation belongs to W00-WU05.

## Repository Structure

```text
cmd/
  api/       HTTP API process entrypoint
  worker/    background worker process entrypoint

internal/
  app/       application composition and shared runtime
  domain/    explicit transactional domain boundaries
  platform/  foundational runtime infrastructure
```

The API and Worker may deploy and scale separately while remaining processes from the same transactional Core codebase. Their existence does not imply separate domain services.

## Requirements

- Go 1.27.x
- PostgreSQL local development platform from atlazora-project W00-WU02

The approved W00-WU02 local PostgreSQL endpoint is:

```text
127.0.0.1:15432
```

## Configuration

Runtime configuration is supplied through process environment variables.

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| ATLAZORA_ENV | No | development | Runtime environment |
| ATLAZORA_HTTP_ADDR | No | :8080 | API listen address |
| ATLAZORA_DATABASE_URL | Yes | none | PostgreSQL connection URL |
| ATLAZORA_SHUTDOWN_TIMEOUT | No | 10s | Graceful shutdown timeout |

Supported environments are development, test, staging, and production.

Use `.env.example` as a local reference only. The application does not automatically read `.env` files.

Never commit real credentials. Inject database credentials through the runtime environment or an approved secret-management mechanism.

For local development, replace the `change-me` placeholder in `.env.example` with the password configured by the governed W00-WU02 local platform.

## Build

```powershell
go build ./...
```

Build individual process binaries when required:

```powershell
go build -o atlazora-api.exe ./cmd/api
go build -o atlazora-worker.exe ./cmd/worker
```

## Formatting

```powershell
go fmt ./...
```

## Static Analysis

```powershell
go vet ./...
```

## Tests

```powershell
go test ./...
```

For an uncached verification run:

```powershell
go test ./... -count=1
```

## Run the API

Set the required runtime environment first. Example for PowerShell:

```powershell
$env:ATLAZORA_ENV = "development"
$env:ATLAZORA_HTTP_ADDR = "127.0.0.1:8080"
$env:ATLAZORA_DATABASE_URL = "postgres://atlazora:<local-password>@127.0.0.1:15432/atlazora?sslmode=disable"
$env:ATLAZORA_SHUTDOWN_TIMEOUT = "10s"
go run ./cmd/api
```

The foundational liveness endpoint is:

```text
GET /health/live
```

Expected successful response:

```json
{"status":"ok"}
```

Stop the API with Ctrl+C. The process handles the signal and performs graceful HTTP and database shutdown.

## Run the Worker

With the same required environment configured:

```powershell
go run ./cmd/worker
```

Stop the Worker with Ctrl+C. The process handles the signal and closes its foundational runtime cleanly.

## PostgreSQL Connectivity

Application startup establishes and verifies PostgreSQL connectivity before the API or Worker reports itself started.

Database connection failures exposed by the foundational database package are intentionally sanitized so connection URLs and credentials are not returned through runtime errors.

W00-WU03 establishes connectivity only. Business schemas, domain migrations, transactional persistence, and Transactional Outbox implementation are intentionally outside this work unit.

## Logging

Runtime logs use structured JSON logging.

Foundational fields include:

- service
- environment
- process

Runtime configuration objects and database credentials must not be logged.

## Graceful Shutdown

The API and Worker use signal-aware lifecycle contexts. Ctrl+C or a supported termination signal requests shutdown. The API stops accepting work through HTTP server shutdown and both processes close the shared runtime before exiting.

## Security Baseline

- No real credentials belong in the repository.
- PostgreSQL credentials are injected through runtime configuration.
- Database connection errors are sanitized.
- PostgreSQL remains the authoritative transactional datastore.
- Dependencies are intentionally minimal.
- Database access must follow least-privilege principles as persistence capabilities are introduced.
- Testing and security verification are required for every work unit.

## W00-WU03 Scope Boundary

This foundation does not implement business-domain behavior, physical domain schemas, migrations, shared external contracts, Transactional Outbox publication, or premature service extraction.

Those capabilities remain assigned to their approved work units and repositories.
