# irods-go-rest

OpenAPI-first Go REST API scaffold for iRODS.

## Project layout

```text
.
├── api/                     # OpenAPI contract and codegen config
├── cmd/irods-go-rest/       # Main application entrypoint
├── internal/app/            # App composition and lifecycle
├── internal/config/         # Environment-based configuration
├── internal/domain/         # API-facing domain models
├── internal/httpapi/        # HTTP routing, handlers, JSON responses
├── internal/irods/          # iRODS integration boundary
└── internal/openapi/        # Generated OpenAPI package target
```

## Why this structure

- Keeps the OpenAPI contract in `api/openapi.yaml` as the source of truth.
- Separates transport concerns from iRODS access logic.
- Gives you a stable service interface while the real iRODS adapter is still evolving.
- Keeps generated code isolated in one package so regeneration stays low-risk.

## Quick start

```bash
go run ./cmd/irods-go-rest
```

Then visit:

- `GET /healthz`
- `GET /api/v1/objects/demo-object`
- `GET /api/v1/collections/demo-collection`

## Environment variables

- `IRODS_REST_ADDR` default: `:8080`
- `IRODS_REST_NAME` default: `iRODS REST API`
- `IRODS_REST_ENV` default: `development`
- `IRODS_ZONE` default: `tempZone`
- `IRODS_HOST` default: `localhost`
- `IRODS_PORT` default: `1247`
- `IRODS_DEFAULT_RESOURCE` default: `demoResc`

## OpenAPI workflow

The generated server/models package is intended to live under `internal/openapi`.

If you want to wire in `oapi-codegen`, this scaffold is ready for it:

```bash
go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest \
  --config api/oapi-codegen.yaml \
  api/openapi.yaml
```

That command is not required for the starter project to compile today, but the spec and config are already in place so you can move to generated types/handlers cleanly.
