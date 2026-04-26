# E2E Tests

This directory is reserved for end-to-end tests that run against the real iRODS REST HTTP service and the docker compose test framework.

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

* `GOREST_E2E_CONFIG_FILE` - required single-file config for HTTP E2E and direct `internal/irods` integration runs; may include both normal app config and an `E2E` section with test-only values
* `GOREST_E2E_BASE_URL` - optional base URL override for the running iRODS REST service; if unset, E2E setup falls back to `E2E.BaseURL` or top-level `PublicURL` from `GOREST_E2E_CONFIG_FILE`
* `DRS_TEST_BEARER_TOKEN` - bearer token for authenticated endpoint tests
* `GOREST_E2E_SKIP_TLS_VERIFY` - optional, set to `true` when the docker test framework uses self-signed TLS
* `GOREST_E2E_BASIC_USERNAME` - optional Basic auth username override; otherwise read from `E2E.BasicUsername`
* `GOREST_E2E_BASIC_PASSWORD` - optional Basic auth password override; otherwise read from `E2E.BasicPassword`
* `GOREST_E2E_IRODS_HOST` - optional direct iRODS host override for fixture upload; if unset, E2E setup falls back to top-level `IrodsHost` from `GOREST_E2E_CONFIG_FILE`
* `GOREST_E2E_IRODS_PORT` - optional direct iRODS port override for fixture upload; if unset, E2E setup falls back to top-level `IrodsPort` from `GOREST_E2E_CONFIG_FILE`
* `GOREST_E2E_IRODS_ZONE` - optional iRODS zone override for fixture upload; if unset, E2E setup falls back to top-level `IrodsZone` from `GOREST_E2E_CONFIG_FILE`
* `GOREST_E2E_IRODS_USER` - optional iRODS fixture-uploader or proxy user; otherwise read from `E2E.IRODSUser`, then falls back to the Basic auth user
* `GOREST_E2E_IRODS_PASSWORD` - optional uploader or proxy password; otherwise read from `E2E.IRODSPassword`, then falls back to the Basic auth password only when no distinct proxy uploader is configured

Both suites require `GOREST_E2E_CONFIG_FILE`. They do not fall back to
`IRODS_REST_CONFIG_FILE` or `e2e/rest-config.e2e.sample.yaml` automatically.

## Shared Config File

When `GOREST_E2E_CONFIG_FILE` is set, the E2E helpers read that file first.
That file may contain:

* the same top-level app config fields used by `irods-go-rest`
* an additional `E2E` section for test-only values such as the `test1` credentials

For the test side, `GOREST_E2E_CONFIG_FILE` is sufficient by itself. The E2E
and direct integration helpers treat it as the default app-config source for
loading:

* `PublicURL`
* `IrodsHost`
* `IrodsPort`
* `IrodsZone`

`IRODS_REST_CONFIG_FILE` is optional only if you also want the separately
running app process to use the same config file. The tests themselves do not
read it as a fallback.

Preferred local workflow:

```bash
export GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml
go test -tags=e2e ./e2e/...
```

The sample config assumes the app is reachable at `http://127.0.0.1:8080`.

Sample combined config:

* [e2e/rest-config.e2e.sample.yaml](/Users/conwaymc/Documents/workspace-gabble/irods-go-rest/e2e/rest-config.e2e.sample.yaml)

## Inputs Not Covered By Current rest-config.yaml

The current checked-in [rest-config.yaml](/Users/conwaymc/Documents/workspace-gabble/irods-go-rest/internal/config/rest-config.yaml)
does not currently populate the following E2E inputs directly:

* `DRS_TEST_BEARER_TOKEN`
* `GOREST_E2E_BASIC_USERNAME`
* `GOREST_E2E_BASIC_PASSWORD`
* `GOREST_E2E_IRODS_USER`
* `GOREST_E2E_IRODS_PASSWORD`
* `GOREST_E2E_SKIP_TLS_VERIFY`

Those values can now be carried in `GOREST_E2E_CONFIG_FILE` under the `E2E`
section instead of being exported individually.

It also currently leaves these app config fields blank in the checked-in file,
so they still need to be supplied by environment variables or by a local
config-file override in real runs:

* `IrodsHost`
* `IrodsPort`
* `IrodsZone`
* `IrodsAdminUser`
* `IrodsAdminPassword`
* `IrodsAuthScheme`
* `IrodsDefaultResource`
* `OidcUrl`
* `OidcRealm`

Default fixture policy:

* use the configured Basic user and that user-owned fixture paths for general E2E and direct integration coverage
* switch to admin or proxy-oriented credentials only when the test is explicitly validating admin-backed or proxy-auth behavior
* do not hard-code a shared collection path for path tests; generate a fresh fixture tree before tests and upload it into a per-run iRODS collection
* when fixture uploader credentials differ from the Basic auth test user, the uploaded fixture should still live under the Basic auth user's home collection so the path tests exercise that user's view
* if `E2E.IRODSUser` differs from `E2E.BasicUsername`, both suites use an iRODS proxy-account shape for direct iRODS setup while still validating the Basic user's view of the path tree

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

The docker-compose-backed test environment is under:

* `deployments/docker-test-framework/5-0`

Use `DEVELOPER_NOTES.md` for the higher-level testing taxonomy and environment setup guidance.
