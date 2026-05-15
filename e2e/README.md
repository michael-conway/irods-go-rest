# E2E Tests

This directory is reserved for end-to-end tests that run against the real iRODS
REST HTTP service and a reachable iRODS test grid.

The preferred local grid is now `irods-grid-stack`. The older
`deployments/docker-test-framework/` stack in this repository is deprecated and
kept only as a compatibility fixture while REST and DRS development workflows
move to the shared grid stack.

These tests are intended to exercise the full stack:

* HTTP routing and middleware
* authentication
* browser and API auth boundaries
* iRODS integration
* Keycloak-backed bearer token flows
* docker-compose-managed runtime dependencies

## Build Tag

End-to-end tests in this directory should use the `e2e` build tag:

```go
//go:build e2e
// +build e2e
```

Run them explicitly:

```bash
go test -tags=e2e ./e2e/...
```

Direct `internal/irods` live integration tests use the `integration` build tag.
They use the same config shape, and both suites now require
`GOREST_E2E_CONFIG_FILE` to be set explicitly:

```bash
go test -tags=integration ./internal/irods
```

## Environment

The current convention for E2E tests is:

* `GOREST_E2E_CONFIG_FILE` - required single-file config for HTTP E2E and direct `internal/irods` integration runs

Both suites require `GOREST_E2E_CONFIG_FILE`. They do not fall back to
`IRODS_REST_CONFIG_FILE` or `e2e/rest-config.e2e.sample.yaml` automatically.

## Shared Config File

When `GOREST_E2E_CONFIG_FILE` is set, the E2E helpers read that file first.
That file contains all E2E/integration inputs in top-level keys.

For the test side, `GOREST_E2E_CONFIG_FILE` is sufficient by itself. The E2E
and direct integration helpers treat it as the default app-config source for
loading:

* `PublicURL`
* `IrodsHost`
* `IrodsPort`
* `IrodsZone`
* `IrodsAuthScheme`
* `IrodsDefaultResource`
* `IrodsAdminUser`
* `IrodsAdminPassword`
* `IrodsPrimaryTestUser`
* `IrodsPrimaryTestPassword`
* `IrodsSecondaryTestUser`
* `IrodsSecondaryTestPassword`
* `TestResource1`
* `TestResource2`
* `TestBearerToken`
* `S3ApiSupported`
* `OidcUrl`
* `OidcClientId`
* `OidcClientSecret`
* `OidcRealm`
* `OidcInsecureSkipVerify`

`IRODS_REST_CONFIG_FILE` is optional only if you also want the separately
running app process to use the same config file. The tests themselves do not
read it as a fallback.

Preferred local workflow:

```bash
export GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml
go test -tags=e2e ./e2e/...
```

The sample config assumes the app is reachable at `http://127.0.0.1:8080` and
uses the default host-facing `irods-grid-stack` ports and resource names. It
expects all test credentials and test settings in top-level fields. The S3
mapping file values are absolute-path placeholders; replace
`/absolute/path/to/irods-grid-stack` with the local grid-stack checkout path
before running S3 admin E2E tests.

Sample combined config:

* [e2e/rest-config.e2e.sample.yaml](/Users/conwaymc/Documents/workspace-gabble/irods-go-rest/e2e/rest-config.e2e.sample.yaml)

## Inputs Not Covered By Current rest-config.yaml

The current checked-in [rest-config.yaml](/Users/conwaymc/Documents/workspace-gabble/irods-go-rest/internal/config/rest-config.yaml)
leaves these E2E/integration values blank:

* `PublicURL`
* `IrodsPrimaryTestUser`
* `IrodsPrimaryTestPassword`
* `IrodsSecondaryTestUser`
* `IrodsSecondaryTestPassword`
* `TestBearerToken`

It also currently leaves these app config fields blank,
so they still need to be supplied by environment variables or by a local
config-file override in real runs:

* `IrodsHost`
* `IrodsPort`
* `IrodsZone`
* `IrodsAdminUser`
* `IrodsAdminPassword`
* `IrodsAdminLoginType`
* `IrodsAuthScheme`
* `IrodsNegotiationPolicy`
* `IrodsDefaultResource`
* `OidcUrl`
* `OidcRealm`

Default fixture policy:

* use `IrodsPrimaryTestUser` and `IrodsPrimaryTestPassword` for Basic auth requests
* use `IrodsAdminUser` and `IrodsAdminPassword` for direct fixture setup/proxy flows
* do not hard-code a shared collection path for path tests; generate a fresh fixture tree before tests and upload it into a per-run iRODS collection
* when fixture uploader credentials differ from the Basic auth test user, the uploaded fixture should still live under the Basic auth user's home collection so the path tests exercise that user's view

## Generated Fixture Tree

Before the path-focused E2E tests run, the suite generates a local source tree under:

* `e2e/resources/test_folder`

The generated source tree is then uploaded into a fresh iRODS collection beneath the E2E fixture user home collection.

Fixture generation rules:

* collections are nested 4 levels deep below the generated root
* each collection gets roughly 8-12 files
* file names and collection names are generated
* file extensions are chosen from common types such as `.txt`, `.md`, `.json`, `.csv`, `.yaml`, `.xml`, `.log`, and `.html`
* file contents are random bytes
* file sizes range from 1 to 100 bytes

## Source of Truth

The docker-compose-backed test environment is `irods-grid-stack`:

```bash
cd ../irods-grid-stack
cp .env.example .env

# Backend-only grid for local REST/DRS development from source.
docker compose up -d --build

# Full demo stack including REST, DRS, and Starbase containers.
docker compose --profile frontend up -d --build
```

Use the backend-only mode when you want to run `irods-go-rest` locally from this
repository while reusing grid-stack iRODS, Keycloak, and S3 services. Use the
`frontend` profile when E2E tests should target the containerized REST service
on the configured host port.

The legacy in-repository compose stack remains under:

* `deployments/docker-test-framework/5-0`

It should not receive new feature work unless a short-term compatibility fix is
needed.

Use `DEVELOPER_NOTES.md` for the higher-level testing taxonomy and environment setup guidance.
