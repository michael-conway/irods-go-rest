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

## Environment

The current convention for E2E tests is:

* `GOREST_E2E_BASE_URL` - base URL of the running iRODS REST service
* `DRS_TEST_BEARER_TOKEN` - bearer token for authenticated endpoint tests
* `GOREST_E2E_SKIP_TLS_VERIFY` - optional, set to `true` when the docker test framework uses self-signed TLS

Tests should skip cleanly when required environment variables are not present.

## Source of Truth

The docker-compose-backed test environment is under:

* `deployments/docker-test-framework/5-0`

Use `DEVELOPER_NOTES.md` for the higher-level testing taxonomy and environment setup guidance.
