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
- `GET /web/`
- `GET /web/login`
- `GET /api/v1/objects/demo-object`
- `GET /api/v1/collections/demo-collection`

## Docker

Build the container locally:

```bash
docker build -t irods-go-rest:local .
```

Run it with the service bound on port `8080`:

```bash
docker run --rm -p 8080:8080 \
  -e GOREST_IRODS_HOST=irods-provider \
  -e GOREST_IRODS_PORT=1247 \
  -e GOREST_OIDC_URL=http://keycloak:8080 \
  -e GOREST_OIDC_REALM=irods \
  -e GOREST_OIDC_CLIENT_ID=irods-go-rest \
  irods-go-rest:local
```

If you want to add this service to the `irods-go-drs` compose stack, this service definition should fit the existing `postgres`, `irods-provider`, and `keycloak` services:

```yaml
  irods-go-rest:
    hostname: irods-go-rest
    platform: linux/amd64
    build:
      context: ../irods-go-rest
      dockerfile: Dockerfile
    image: irods-go-rest:local
    depends_on:
      irods-provider:
        condition: service_started
      keycloak:
        condition: service_started
    environment:
      GOREST_PUBLIC_URL: http://irods-go-rest:8080
      GOREST_REST_LOG_LEVEL: info
      GOREST_IRODS_ZONE: tempZone
      GOREST_IRODS_HOST: irods-provider
      GOREST_IRODS_PORT: 1247
      GOREST_IRODS_DEFAULT_RESOURCE: demoResc
      GOREST_OIDC_URL: http://keycloak:8080
      GOREST_OIDC_REALM: drs
      GOREST_OIDC_CLIENT_ID: irods-go-rest
      GOREST_OIDC_CLIENT_SECRET: ""
    ports:
      - "8081:8080"
```

Adjust the host port, realm, and client settings to match your Keycloak import and whichever external URL you want browsers to use for `/web/login`.

## Configuration

The service reads `rest-config.yaml` plus `GOREST_*` environment variables. Environment variables override file values.

- `GOREST_PUBLIC_URL` example: `http://localhost:8080`
- `GOREST_REST_LOG_LEVEL` default: `info`
- `GOREST_IRODS_ZONE` example: `tempZone`
- `GOREST_IRODS_HOST` example: `localhost`
- `GOREST_IRODS_PORT` default: `1247`
- `GOREST_IRODS_ADMIN_USER` example: `rods`
- `GOREST_IRODS_ADMIN_PASSWORD` optional, but prefer `GOREST_IRODS_ADMIN_PASSWORD_FILE`
- `GOREST_IRODS_ADMIN_PASSWORD_FILE` path to a mounted secret file
- `GOREST_IRODS_DEFAULT_RESOURCE` example: `demoResc`
- `GOREST_OIDC_URL` example: `http://localhost:8081`
- `GOREST_OIDC_REALM` example: `irods`
- `GOREST_OIDC_CLIENT_ID` example: `irods-go-rest`
- `GOREST_OIDC_CLIENT_SECRET` optional, but prefer `GOREST_OIDC_CLIENT_SECRET_FILE`
- `GOREST_OIDC_CLIENT_SECRET_FILE` path to a mounted secret file
- `GOREST_OIDC_SCOPE` default: `openid profile email`

## Browser login flow

```bash
open http://localhost:8080/web/login
```

The companion web flow redirects the browser to Keycloak's authorization endpoint. After sign-in, Keycloak returns to `/web/callback`, where the service exchanges the code, verifies the token with Keycloak, and creates an HTTP-only browser session for the web companion flow.

## API auth model

The REST API is a resource server. It expects an `Authorization: Bearer ...` token on protected endpoints and validates that token against Keycloak before serving the request.

```bash
curl http://localhost:8080/api/v1/objects/demo-object \
  -H 'Authorization: Bearer YOUR_ACCESS_TOKEN'
```

This keeps the browser login flow separate from the API contract, which is a better fit for a DRS-adjacent service.

## OpenAPI workflow

The generated server/models package is intended to live under `internal/openapi`.

If you want to wire in `oapi-codegen`, this scaffold is ready for it:

```bash
go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest \
  --config api/oapi-codegen.yaml \
  api/openapi.yaml
```

That command is not required for the starter project to compile today, but the spec and config are already in place so you can move to generated types/handlers cleanly.
