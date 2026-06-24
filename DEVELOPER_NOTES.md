# Developer Notes

These notes capture the maintainer rules for `irods-go-rest`. User-facing setup belongs in [README.md](./README.md); runtime configuration belongs in [CONFIGURATION_NOTES.md](./CONFIGURATION_NOTES.md).

## API Boundaries

Keep the public API path-oriented:

- `irods_path` stays in the query string for full iRODS logical paths.
- Generic iRODS operations stay under `/api/v1/path*`, `/api/v1/user*`, `/api/v1/usergroup*`, `/api/v1/ticket*`, and `/api/v1/server`.
- Workflow-specific APIs stay under `/api/v1/ext/*`.
- Do not introduce parallel Keycloak-specific user/group routes when the generic iRODS user and group routes express the operation.

Extension support signaling:

- `501 not_supported`: capability intentionally disabled in this deployment.
- `503 not_configured`: capability enabled but required deployment configuration is missing.
- `403 permission_denied`: authenticated caller lacks iRODS authority.

Prefer one REST origin for clients. Split functionality into a sidecar only when independent lifecycle, scale, or security isolation is a concrete requirement.

## Code Layout

Keep responsibilities separated:

- `api/openapi.yaml` is the HTTP contract source of truth.
- `internal/httpapi/` owns routing, request parsing, auth middleware, links, and response mapping.
- `internal/restservice/` adapts request context to service calls.
- `internal/irods/` owns iRODS behavior and go-irodsclient integration.
- `internal/domain/` owns API-facing response models.
- `internal/config/` owns config loading and environment binding.

Do not push URL generation, HTTP status mapping, or handler-specific concerns into the iRODS layer.

## Shared Client Logic

Monitor duplicated higher-level iRODS workflows against `go-irodsclient-extensions`.

Move logic into `go-irodsclient-extensions` when it is:

- useful to both `irods-go-rest` and `irods-go-drs`
- not HTTP-specific
- stable enough to carry as shared client behavior

Keep service-local behavior in this repository when it depends on HTTP request context, response shaping, or REST-specific authorization policy.

## Auth Policy

API routes remain stateless at request time:

- Bearer tokens are validated through OIDC/Keycloak.
- Basic auth maps directly to iRODS user credentials.
- `Bearer irods-ticket:<ticket>` is accepted only for content download flows.
- `/web/*` browser login uses server-side sessions, but `/api/v1/*` must not depend on web session state.

Bearer token acquisition options targeted for beta:

- Authorization Code + PKCE for browser clients.
- Device Code flow for CLI sign-in.
- Client Credentials for service-to-service automation.
- Token exchange when scoped downstream tokens are required.

Keycloak admin/scripted token minting is a development convenience, not production UX.

## User And Group Sync

`reconcile=true` user and usergroup calls use normal iRODS authority. REST does not maintain a separate sync ACL or service-account state store.

Service-account bearer callers follow the same path as other bearer callers: validate token, resolve effective/proxy iRODS account, then let iRODS user type determine whether catalog mutation can proceed.

Keep sync policy in `go-irodsclient-extensions/usersync` where possible:

- sync assumes an already-authorized iRODS filesystem
- sync may manage normal `rodsuser` accounts and `rodsgroup` groups
- sync must not manage `groupadmin` or `rodsadmin` users
- iRODS privilege changes stay outside sync and should be handled through iRODS admin workflows

Durable sync ownership state must be iRODS-native AVUs:

- `iRODS:USER_SYNCH:MANAGED`
- `iRODS:USER_SYNCH:SOURCE`
- `iRODS:USER_SYNCH:REALM`
- `iRODS:USER_SYNCH:EXTERNAL_ID`
- `iRODS:USER_SYNCH:LAST_SYNC_AT`
- `iRODS:USER_SYNCH:LAST_PLAN_ID`

### Starbase Users And Groups Administration

`starbase` is adding a top-level `Users & Groups` function backed by the
generic user and usergroup APIs. Preserve the current generic route family as
the first contract for this UI:

- `GET /api/v1/user`
- `POST /api/v1/user`
- `GET /api/v1/user/{user_name}`
- `PUT /api/v1/user/{user_name}`
- `DELETE /api/v1/user/{user_name}`
- `GET /api/v1/user/{user_name}/avu`
- `POST /api/v1/user/{user_name}/avu`
- `PUT /api/v1/user/{user_name}/avu/{avu_id}`
- `DELETE /api/v1/user/{user_name}/avu/{avu_id}`
- `GET /api/v1/usergroup`
- `POST /api/v1/usergroup`
- `GET /api/v1/usergroup/{group_name}`
- `DELETE /api/v1/usergroup/{group_name}`
- `GET /api/v1/usergroup/{group_name}/avu`
- `POST /api/v1/usergroup/{group_name}/avu`
- `PUT /api/v1/usergroup/{group_name}/avu/{avu_id}`
- `DELETE /api/v1/usergroup/{group_name}/avu/{avu_id}`
- `POST /api/v1/usergroup/{group_name}/member`
- `DELETE /api/v1/usergroup/{group_name}/member/{user_name}`

Frontend requirements to watch:

- Basic read-only tables can use the existing list endpoints.
- User autocomplete and group autocomplete should keep using prefix filters on
  the generic list routes.
- User/group core updates are intentionally narrow. Use principal AVU routes
  for editable user/group annotations instead of adding generic group update
  routes without a concrete catalog field requirement.
- Starbase must not perform broad client-side joins, catalog-wide filtering, or
  repeated per-row group detail fetches for list-scale views.
- If the UI needs group member counts, users with membership summaries, groups
  containing a given user, or principal search across users and groups, add a
  documented REST route here instead of pushing that logic into the browser.

Likely efficient API candidates:

- a GenQuery-backed group summary route that returns group rows with member
  counts for list views
- a GenQuery-backed user membership summary route that returns each user with
  zero or more group names
- a GenQuery-backed reverse membership route for groups containing one selected
  user
- a principal search route only if a combined user/group search is demonstrably
  better than the existing separate `/api/v1/user` and `/api/v1/usergroup`
  prefix searches

Any new route must be added to `api/openapi.yaml`, mapped through
`internal/httpapi/`, shaped in `internal/domain/`, and implemented in the
service/catalog layers with the same auth and error semantics as the existing
generic user and usergroup endpoints. Prefer explicit request parameters and
response fields over exposing raw GenQuery syntax to clients.

## Request Context And Visibility

Carry audit context through logs and service calls using:

- `X-Request-ID`
- `X-IRODS-Source`
- `X-IRODS-Actor`
- `Idempotency-Key`
- bearer subject, client ID, scope, and audience when present

If `X-Request-ID` is absent, REST generates one and returns it in the response header.

Be aware of cross-session iRODS read-after-write visibility. This service currently handles create, rename, and delete by doing short fresh-session postcondition checks before returning success. Keep that workaround in the service layer until go-irodsclient exposes a better no-cache/fresh-read API.

## Testing

Use three layers:

```bash
GOWORK=off go test ./...
```

```bash
GOWORK=off GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml \
  go test -tags=integration ./internal/irods
```

```bash
GOWORK=off GOREST_E2E_CONFIG_FILE=./e2e/rest-config.e2e.sample.yaml \
  go test -tags=e2e ./e2e/...
```

Both live-test suites load settings from `GOREST_E2E_CONFIG_FILE`, including test users, admin credentials, iRODS resources, OIDC settings, and `TestBearerToken`.

Prefer `irods-grid-stack` for local live-test infrastructure. The older in-repository Docker test framework under `deployments/docker-test-framework/5-0` is a compatibility fixture only.

## Local Multi-Repo Development

Use a workspace `go.work` file for cross-repo development instead of committing local `replace` directives.

Typical workspace at `workspace-gabble/go.work`:

- `./go-irodsclient-extensions`
- `./irods-go-rest`
- `./irods-go-drs`

Workflow:

1. Develop with `go.work` active locally.
2. Keep each repository `go.mod` pinned to real module versions.
3. Tag shared changes in `go-irodsclient-extensions`.
4. Bump dependent repositories with `GOWORK=off go get <module>@<tag-or-commit>`.
5. Run `GOWORK=off go mod tidy` and tests in each dependent repository.

## Release Checklist

## Known Client Gaps

- Checksum operations still require lower-level go-irodsclient calls with a metadata connection. A first-class checksum API in `fs.FileSystem` would simplify this service.
- Ticket helper workflows should be extracted into `go-irodsclient-extensions` when they are not HTTP-specific.
- go-irodsclient caching does not yet provide a public no-cache or explicit fresh-read API for path existence/lookups.
