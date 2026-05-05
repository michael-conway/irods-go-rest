# iRODS Go REST API

[![Go](https://github.com/michael-conway/irods-go-rest/actions/workflows/go.yml/badge.svg)](https://github.com/michael-conway/irods-go-rest/actions/workflows/go.yml)
[![Container Build](https://github.com/michael-conway/irods-go-rest/actions/workflows/container-build.yml/badge.svg)](https://github.com/michael-conway/irods-go-rest/actions/workflows/container-build.yml)
[![CodeQL Advanced](https://github.com/michael-conway/irods-go-rest/actions/workflows/codeql.yml/badge.svg)](https://github.com/michael-conway/irods-go-rest/actions/workflows/codeql.yml)

OpenAPI Go REST API for iRODS.

## Overview

This project provides a Go-based REST service for iRODS with an OpenAPI-defined HTTP interface, bearer/basic authentication support, browser-based login flow support, and a growing set of endpoints for logical-path lookup, child listing, and byte streaming.

It includes:

* a REST API for iRODS logical-path-oriented access
* OpenAPI source documents and Swagger UI
* HTTP middleware for bearer token, basic auth, and ticket-oriented download flows
* iRODS service-layer boundaries for catalog and content operations
* unit-testable handler, auth, and service scaffolding for further development

## Project Metadata

| Field | Value |
| --- | --- |
| Project Name | `iRODS Go REST API` |
| Current Version | `TBD` |
| Status | `Active Development` |
| Primary Developer | `Mike Conway` |
| Organization | `NIEHS` |
| Repository | `https://github.com/michael-conway/irods-go-rest` |
| Contact | `mike.conway@nih.gov` |
| Issue Tracker | `https://github.com/michael-conway/irods-go-rest/issues` |
| License | `TBD` |

## Master Index

* [Configuration Notes](./CONFIGURATION_NOTES.md)
* [Developer Notes](./DEVELOPER_NOTES.md)
* [OpenAPI Contract](./api/openapi.yaml)

## Project Structure

The repository follows a conventional Go layout centered around an OpenAPI-described HTTP service, HTTP/auth middleware, and iRODS service boundaries.

| Path | Purpose |
| --- | --- |
| `cmd/irods-go-rest/` | Main application entrypoint |
| `internal/app/` | App composition and service lifecycle |
| `internal/config/` | Runtime configuration and environment binding |
| `internal/auth/` | Keycloak-backed auth flow and token verification support |
| `internal/httpapi/` | HTTP routing, middleware, handlers, and responses |
| `internal/irods/` | iRODS integration boundary and service scaffolding |
| `internal/domain/` | API-facing domain models |
| `api/` | OpenAPI contract and embedding support |
| `e2e/` | Docker-compose-backed end-to-end HTTP tests |
| `deployments/` | Docker and local development deployment assets |

## Stack and Testing Strategy

The implementation is written in Go and keeps the OpenAPI document in `api/openapi.yaml` as the source of truth for the HTTP contract. The HTTP layer is intentionally separated from the iRODS service layer so routing, auth, and response behavior can evolve without pushing transport concerns into the backend access code.

Testing is currently centered on package-local unit tests, especially in the HTTP layer:

* handler and middleware tests live next to the code they validate
* HTTP behavior is exercised with `httptest`
* auth flow and token verification behavior is tested independently from browser login flow support
* the iRODS adapter remains scaffolded so the contract can evolve before binding fully to go-irodsclient

For docker-compose-backed HTTP system testing, `irods-go-rest` now also reserves `e2e/` for explicit end-to-end tests using the `e2e` build tag.

## Quick Start

Run the service locally:

```bash
go run ./cmd/irods-go-rest
```

Then visit:

* `GET /healthz`
* `GET /swagger`
* `GET /openapi.yaml`
* `GET /web/`
* `GET /web/login`

## API Documentation

When the service is running locally on the default port, API documentation is available at:

* Swagger UI: `http://localhost:8080/swagger`
* OpenAPI spec: `http://localhost:8080/openapi.yaml`

## Configuration

The service reads `rest-config.yaml` plus `GOREST_*` environment variables. Environment variables override file values.

Common settings include:

* `GOREST_PUBLIC_URL`
* `GOREST_REST_LOG_LEVEL`
* `GOREST_IRODS_ZONE`
* `GOREST_IRODS_HOST`
* `GOREST_IRODS_PORT`
* `GOREST_IRODS_ADMIN_USER`
* `GOREST_IRODS_ADMIN_PASSWORD`
* `GOREST_IRODS_ADMIN_PASSWORD_FILE`
* `GOREST_IRODS_DEFAULT_RESOURCE`
* `GOREST_RESOURCE_AFFINITY`
* `GOREST_OIDC_URL`
* `GOREST_OIDC_REALM`
* `GOREST_OIDC_CLIENT_ID`
* `GOREST_OIDC_CLIENT_SECRET`
* `GOREST_OIDC_CLIENT_SECRET_FILE`
* `GOREST_OIDC_SCOPE`
* `GOREST_OIDC_INSECURE_SKIP_VERIFY`

`GOREST_RESOURCE_AFFINITY` is optional and accepts a comma-separated list of
iRODS resource names that are proximate to this service instance.

If you want to point the service at one explicit config file, use:

```bash
IRODS_REST_CONFIG_FILE=/path/to/rest-config.yaml
```

See [Configuration Notes](./CONFIGURATION_NOTES.md) for runtime and Docker-oriented configuration details.

## Auth Model

Protected API endpoints currently accept either:

* `Authorization: Bearer <token>`
* `Authorization: Basic <base64(user:password)>`

Bearer tokens are validated against Keycloak. Basic credentials are accepted by the HTTP middleware as an alternate API auth style and are intended to align with future iRODS-backed validation work.

For download-oriented content endpoints, the service also accepts:

* `Authorization: Bearer irods-ticket:<ticket>`

This keeps the download contract compatible with future DRS-issued ticket flows without changing the endpoint shape later.

The browser login flow remains separate under `/web/*`, which keeps the API contract cleaner and easier to use as a machine-facing service.

## Path Model

For iRODS resources whose true identifier is the full iRODS logical path, the API treats that path as request data rather than embedding it in the URL route.

Current path-based endpoints:

* Path metadata:
  `GET /api/v1/path?irods_path=/tempZone/home/test1/file.txt`
* Collection children:
  `GET /api/v1/path/children?irods_path=/tempZone/home/test1/project`
* AVU metadata:
  `GET /api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt`
* Add AVU metadata:
  `POST /api/v1/path/avu?irods_path=/tempZone/home/test1/file.txt`
* Update or delete a single AVU:
  `PUT /api/v1/path/avu/{avu_id}?irods_path=/tempZone/home/test1/file.txt`
  `DELETE /api/v1/path/avu/{avu_id}?irods_path=/tempZone/home/test1/file.txt`
* Content headers:
  `HEAD /api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt`
* Content bytes:
  `GET /api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt`

Additional endpoint:

* iRODS server information (miscsvrinfo-style plus configured connection details):
  `GET /api/v1/server`

`/path` is the primary lookup model for both data objects and collections. The response identifies what the path resolves to using a discriminator such as `kind: data_object` or `kind: collection`.

Path responses also include a `parent` field when a parent exists. This follows a lightweight HATEOAS pattern by exposing the parent iRODS path along with the REST link that can be followed to retrieve that parent:

* `parent.irods_path`
* `parent.href`

Collection-specific behavior is expressed through subresources such as `/path/children`. AVU metadata is exposed as `/path/avu` so clients can list, create, update, and delete metadata rows while preserving `irods_path` as the resource identifier. Data-object-specific behavior is expressed through subresources such as `/path/contents`.

This establishes `/path` as the generic REST pattern for logical-path-oriented operations. Additional routes such as `/path/metadata` and `/path/acl` can be added later without changing the core addressing model.

## Extension Endpoint Policy

Opinionated, workflow-specific APIs should live in this service under an explicit extension namespace:

* `/api/v1/ext/*`

This keeps the core API surface (`/api/v1/path`, `/api/v1/server`, and related generic resources) focused on broadly reusable iRODS operations, while still allowing higher-level features such as file carts to be exposed without introducing a second public service.

Current architectural preference:

* keep one public REST origin for clients
* reuse the same auth and CORS behavior as core endpoints
* gate extension features by configuration when needed

Only move extension functionality to a separate sidecar service when there is a clear need for independent deployment, scaling, or security isolation.

The content endpoint supports restart/resume through the standard HTTP `Range` header.

Example:

```bash
curl \
  -H 'Authorization: Bearer YOUR_ACCESS_TOKEN' \
  -H 'Range: bytes=1024-' \
  'http://localhost:8080/api/v1/path/contents?irods_path=/tempZone/home/test1/file.txt'
```

## Docker

Build the container locally:

```bash
docker build -t irods-go-rest:local .
```

Run it with the service bound on port `8080`:

```bash
docker run --rm -p 8080:8080 \
  -e IRODS_REST_ADDR=:8080 \
  -e GOREST_IRODS_HOST=irods-provider \
  -e GOREST_IRODS_PORT=1247 \
  -e GOREST_OIDC_URL=http://keycloak:8080 \
  -e GOREST_OIDC_REALM=irods \
  -e GOREST_OIDC_CLIENT_ID=irods-go-rest \
  irods-go-rest:local
```

This service can also be added to the `irods-go-drs` docker-compose-based development stack. See [Developer Notes](./DEVELOPER_NOTES.md) for the current compose assumptions and runtime environment expectations.

## OpenAPI Workflow

If you want to regenerate code from the OpenAPI contract, the repository is already structured for it:

```bash
go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest \
  --config api/oapi-codegen.yaml \
  api/openapi.yaml
```

The service does not require regeneration to compile today, but the contract and layout are set up so generated code can be introduced cleanly.

## References

* OpenAPI Specification: https://github.com/OAI/OpenAPI-Specification
* oapi-codegen: https://github.com/oapi-codegen/oapi-codegen
* go-irodsclient: https://github.com/cyverse/go-irodsclient
* Keycloak: https://www.keycloak.org/documentation
* Go standard `net/http`: https://pkg.go.dev/net/http
* Go standard `httptest`: https://pkg.go.dev/net/http/httptest
* Viper: https://github.com/spf13/viper
* Zerolog: https://github.com/rs/zerolog
