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

For opinionated features, keep them in this repository but place them under:

- `/api/v1/ext/*`

Use this boundary:

- core generic iRODS operations stay under `/api/v1/path*` and related generic resources
- opinionated workflow endpoints (for example favorites, metadata manifests, and S3 admin helpers) stay under `/api/v1/ext/*`

Extension support signaling:

- return `501` with `code=not_supported` when an extension capability is intentionally unavailable in the current deployment mode
- return `503` with `code=not_configured` when the feature is supported but missing required deployment configuration (for example S3 mapping files)

Default preference is one service/origin to avoid extra CORS/auth/gateway complexity for clients.
Only split to a sidecar if there is a concrete need for independent lifecycle, scaling, or security isolation.

## Code layout

Keep the code split this way:

- `internal/httpapi/` handles routing, auth, and HTTP response mapping
- `internal/irods/` handles iRODS lookup and content behavior
- `api/openapi.yaml` is the contract source of truth

Do not push URL-building or handler concerns into the iRODS layer.

Monitor shared higher-level iRODS client functionality against `go-irodsclient-extensions`.

If logic here is also needed by `irods-go-drs` or other clients, prefer refactoring it into `go-irodsclient-extensions` instead of keeping duplicated copies in service repositories.

## Auth

Current API auth supports:

- Bearer tokens
- Basic auth
- `Bearer irods-ticket:<ticket>` on content endpoints

Browser login stays under `/web/*`.

### User/Group Sync Authorization

`reconcile=true` user and usergroup calls follow normal iRODS authority. REST
does not maintain a separate sync ACL or service-account state store.

Service-account callers are handled by the same path as other bearer callers:
the token is validated, REST resolves the effective/proxy iRODS account, and
the iRODS user type determines whether catalog mutations may proceed. Service
accounts that need sync authority must map to an underlying iRODS user with the
required iRODS role.

Keep sync policy in `go-irodsclient-extensions/usersync` where possible:

- `usersync` assumes an already-authorized iRODS filesystem
- sync may create/manage normal `rodsuser` accounts and `rodsgroup` groups
- sync must not create, claim, delete, update, or manage memberships for
  `groupadmin` or `rodsadmin` users
- iRODS administrative privilege adjustments remain outside sync and should be
  performed through iRODS admin workflows such as iCommands

Durable sync ownership state must be iRODS-native AVUs using:

- `iRODS:USER_SYNCH:MANAGED`
- `iRODS:USER_SYNCH:SOURCE`
- `iRODS:USER_SYNCH:REALM`
- `iRODS:USER_SYNCH:EXTERNAL_ID`
- `iRODS:USER_SYNCH:LAST_SYNC_AT`
- `iRODS:USER_SYNCH:LAST_PLAN_ID`

REST request audit context is carried through logs and service calls using
`X-Request-ID`, `X-IRODS-Source`, `X-IRODS-Actor`, `Idempotency-Key`, and bearer
auth subject/client/scope/audience when present. If `X-Request-ID` is absent,
REST generates one and returns it in the response header.

## Testing

Use three layers:

- unit tests in the package, run with `go test ./...`
- direct iRODS integration tests under `internal/irods` with `go test -tags=integration ./internal/irods`
- HTTP end-to-end tests under `e2e/` with `go test -tags=e2e ./e2e/...`

Shared live-test variable:

- `GOREST_E2E_CONFIG_FILE`

Both `-tags=e2e` and `-tags=integration` suites now load all test settings from
that single config file using top-level keys (including primary/secondary test
users, admin credentials, and `TestBearerToken`).

## Docker test stack

Prefer `irods-grid-stack` for current local development and live-test runs.
It can run as a backend-only grid for source-level REST/DRS development, or as
a full demo stack with REST, DRS, and Starbase containers.

Backend-only grid:

```bash
cd ../irods-grid-stack
cp .env.example .env
docker compose up -d --build
```

Full demo stack:

```bash
docker compose --profile frontend up -d --build
```

For host-run tests in this repository, use a host-facing config such as:

```bash
export GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml
```

The older in-repository Docker test framework remains under
`deployments/docker-test-framework/5-0` as a compatibility fixture only. Do not
add new environment features there unless a short-term compatibility fix is
needed.

## Working rules

- Preserve the `/path` model.
- Prefer one generic path lookup plus subresources over separate top-level file and collection endpoints.
- Add HATEOAS links when they improve navigation.
- If `go-irodsclient` gets in the way, record the gap here instead of hiding it in commit history.
- Be aware of cross-session read-after-write visibility when one session mutates iRODS and a different session immediately reads it back. `irods-go-rest` currently absorbs this for create, rename, and delete by doing short fresh-session postcondition checks before returning success. Keep this workaround in the service layer, not in `starbase` or other clients.

## Go client notes

Current gap:

- Checksum operations still require dropping below the high-level `fs.FileSystem` API and calling lower-level iRODS functions with a metadata connection. A first-class checksum API in `go-irodsclient/fs.FileSystem` would simplify this service.
- Ticket parsing, ticket creation helpers, and other reusable client workflows should be monitored for extraction into `go-irodsclient-extensions` when they are not HTTP-specific.
- `go-irodsclient/fs.FileSystem` currently exposes caching but not a true public no-cache mode or explicit fresh-read APIs for path existence/lookups. This can surface as immediate cross-session visibility lag after create, rename, or delete. Track this upstream so the long-term fix can move down into `go-irodsclient` rather than staying as polling logic in service repositories.

## Local multi-repo sync (`go.work`)

Use a workspace `go.work` file for local cross-repo development instead of
`replace ../...` directives in `go.mod`.

Current workspace scaffold (at `workspace-gabble/go.work`) includes:

- `./go-irodsclient-extensions`
- `./irods-go-rest`
- `./irods-go-drs`

Workflow:

1. develop across repos with `go.work` active
2. keep each repo `go.mod` pinned to real module versions (no local replace)
3. when shared changes are ready, push/tag in `go-irodsclient-extensions`
4. bump dependent repos with `go get <module>@<tag-or-commit>` and `go mod tidy`
