# iRODS Go REST API

[![Go](https://github.com/michael-conway/irods-go-rest/actions/workflows/go.yml/badge.svg)](https://github.com/michael-conway/irods-go-rest/actions/workflows/go.yml)
[![Container Build](https://github.com/michael-conway/irods-go-rest/actions/workflows/container-build.yml/badge.svg)](https://github.com/michael-conway/irods-go-rest/actions/workflows/container-build.yml)
[![CodeQL Advanced](https://github.com/michael-conway/irods-go-rest/actions/workflows/codeql.yml/badge.svg)](https://github.com/michael-conway/irods-go-rest/actions/workflows/codeql.yml)

`irods-go-rest` is an alpha REST service for iRODS. It exposes an OpenAPI-defined HTTP API for logical path access, data-object content, AVU metadata, users, groups, tickets, server information, and selected extension workflows.

## Status

| Field | Value |
| --- | --- |
| Release | `1.0.0-alpha` |
| Stability | Alpha |
| License | `BSD-2-Clause` |
| Repository | `https://github.com/michael-conway/irods-go-rest` |
| Issues | `https://github.com/michael-conway/irods-go-rest/issues` |

The `1.0.0-alpha` release is intended for integration testing and early deployment work. The route model is stable enough for client development, but behavior and response shapes may still change before a final `1.0.0` release.

## Quick Start

Run from source:

```bash
go run ./cmd/irods-go-rest
```

Then open:

- `http://localhost:8080/healthz`
- `http://localhost:8080/swagger`
- `http://localhost:8080/openapi.yaml`

Use a specific config file with:

```bash
IRODS_REST_CONFIG_FILE=/path/to/rest-config.yaml go run ./cmd/irods-go-rest
```

Configuration details live in [CONFIGURATION_NOTES.md](./CONFIGURATION_NOTES.md).

## Runtime Model

The API is path-oriented. Full iRODS logical paths are request data, not URL path segments:

```http
GET /api/v1/path?irods_path=/tempZone/home/test1/file.txt
GET /api/v1/path/children?irods_path=/tempZone/home/test1/project
GET /api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt
GET /api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt
```

Protected API endpoints accept:

- `Authorization: Bearer <token>`
- `Authorization: Basic <base64(user:password)>`
- `Authorization: Bearer irods-ticket:<ticket>` for content downloads

Bearer tokens are validated through OIDC/Keycloak. Basic auth is direct iRODS user authentication. Browser login support is separate under `/web/*` and is enabled only when `GOREST_WEB_ENABLED=true`.

## Main API Areas

| Area | Routes |
| --- | --- |
| Path lookup and mutation | `/api/v1/path*` |
| Data object contents | `/api/v1/path/contents` |
| Replicas | `/api/v1/path/replicas` |
| AVUs | `/api/v1/path/avu*` |
| ACLs | `/api/v1/path/acl*` |
| Tickets | `/api/v1/ticket*`, `/api/v1/path/ticket` |
| Users | `/api/v1/user*` |
| Groups | `/api/v1/usergroup*` |
| Server info | `/api/v1/server` |
| Extensions | `/api/v1/ext/*` |

See [api/openapi.yaml](./api/openapi.yaml) for the HTTP contract.

## Extension Support

Extension endpoints are workflow-specific APIs under `/api/v1/ext/*`. They share the same auth and CORS policy as the core API.

| Extension | Routes | Alpha status |
| --- | --- | --- |
| Favorites | `/api/v1/ext/favorites` | Stable alpha |
| Metadata manifest | `/api/v1/ext/metadata-manifest` | Stable alpha |
| Metadata queries | `/api/v1/ext/metadata-queries` | Stable alpha |
| S3 bucket admin | `/api/v1/ext/s3/buckets*` | Deployment-gated alpha |
| S3 user-secret admin | `/api/v1/ext/s3/user-secrets*` | Deployment-gated alpha |

Deployment-gated endpoints return:

- `501 not_supported` when the extension is disabled for the deployment.
- `503 not_configured` when required mapping files or runtime settings are missing.
- `403 permission_denied` when the authenticated caller lacks the required iRODS authority.

## Local Test Stack

Use [`irods-grid-stack`](https://github.com/michael-conway/irods-grid-stack) for live iRODS, Keycloak, S3 API, DRS, and Starbase workflows.

Backend-only grid:

```bash
cd ../irods-grid-stack
cp .env.example .env
docker compose up -d --build
```

Full frontend stack:

```bash
docker compose --profile frontend up -d --build
```

For host-run integration or E2E tests, point `GOREST_E2E_CONFIG_FILE` at a host-facing config:

```bash
export GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml
```

## Tests

Unit tests:

```bash
GOWORK=off go test ./...
```

Direct iRODS integration tests:

```bash
GOWORK=off GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml \
  go test -tags=integration ./internal/irods
```

HTTP E2E tests:

```bash
GOWORK=off GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml \
  go test -tags=e2e ./e2e/...
```

## Container Images

Build locally:

```bash
docker build -t irods-go-rest:local .
```

Published images use GitHub Container Registry:

```text
ghcr.io/michael-conway/irods-go-rest:<tag>
```

The container workflow publishes branch tags, SHA tags, `latest` for the default branch, and release tags. Publishing the GitHub release `1.0.0-alpha` publishes:

```text
ghcr.io/michael-conway/irods-go-rest:1.0.0-alpha
```

## Developer References

- [DEVELOPER_NOTES.md](./DEVELOPER_NOTES.md) - implementation rules, testing layers, and release checklist
- [CONFIGURATION_NOTES.md](./CONFIGURATION_NOTES.md) - runtime configuration reference
- [api/openapi.yaml](./api/openapi.yaml) - OpenAPI source of truth
- [go-irodsclient](https://github.com/cyverse/go-irodsclient)
- [go-irodsclient-extensions](https://github.com/michael-conway/go-irodsclient-extensions)
