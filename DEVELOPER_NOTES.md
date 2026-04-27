# Developer Notes

Use this file for the main working rules in `irods-go-rest`.

## API model

The service is path-oriented.

Use:

- `GET /api/v1/path?irods_path=...` for generic path lookup
- `/api/v1/path/children` for collection children
- `/api/v1/path/contents` for data object bytes
- `/api/v1/path/avu` for path-scoped AVU metadata rows

Keep `irods_path` as the identifier input. Do not move full iRODS paths into URL path segments unless there is a strong reason.

For AVUs, keep the path as `irods_path` and the AVU row identifier as the child resource identifier:

- `GET /api/v1/path/avu?irods_path=...`
- `POST /api/v1/path/avu?irods_path=...`
- `PUT /api/v1/path/avu/{avu_id}?irods_path=...`
- `DELETE /api/v1/path/avu/{avu_id}?irods_path=...`

## Code layout

Keep the code split this way:

- `internal/httpapi/` handles routing, auth, and HTTP response mapping
- `internal/irods/` handles iRODS lookup and content behavior
- `api/openapi.yaml` is the contract source of truth

Do not push URL-building or handler concerns into the iRODS layer.

## Auth

Current API auth supports:

- Bearer tokens
- Basic auth
- `Bearer irods-ticket:<ticket>` on content endpoints

Browser login stays under `/web/*`.

## Testing

Use three layers:

- unit tests in the package, run with `go test ./...`
- direct iRODS integration tests under `internal/irods` with `go test -tags=integration ./internal/irods`
- HTTP end-to-end tests under `e2e/` with `go test -tags=e2e ./e2e/...`

Shared live-test variables:

- `GOREST_E2E_CONFIG_FILE`
- `GOREST_E2E_BASE_URL`
- `DRS_TEST_BEARER_TOKEN`
- `GOREST_E2E_SKIP_TLS_VERIFY`
- `GOREST_E2E_BASIC_USERNAME`
- `GOREST_E2E_BASIC_PASSWORD`
- `GOREST_E2E_IRODS_HOST`
- `GOREST_E2E_IRODS_PORT`
- `GOREST_E2E_IRODS_ZONE`
- `GOREST_E2E_IRODS_USER`
- `GOREST_E2E_IRODS_PASSWORD`

Use `GOREST_E2E_CONFIG_FILE` as the main source for live-test configuration, including top-level OIDC settings.

## Docker test stack

The local Docker test framework is under:

```text
deployments/docker-test-framework/5-0
```

It is for development and testing, not production.

Typical local flow:

```bash
cd deployments/docker-test-framework/5-0
docker compose build
docker compose up
```

## Working rules

- Preserve the `/path` model.
- Prefer one generic path lookup plus subresources over separate top-level file and collection endpoints.
- Add HATEOAS links when they improve navigation.
- If `go-irodsclient` gets in the way, record the gap here instead of hiding it in commit history.

## Go client notes

Current gap:

- Checksum operations still require dropping below the high-level `fs.FileSystem` API and calling lower-level iRODS functions with a metadata connection. A first-class checksum API in `go-irodsclient/fs.FileSystem` would simplify this service.
