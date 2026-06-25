# Developer Notes

These notes capture the maintainer rules for `irods-go-rest`. User-facing setup belongs in [README.md](./README.md); runtime configuration belongs in [CONFIGURATION_NOTES.md](./CONFIGURATION_NOTES.md).

## API Boundaries

Keep the public API path-oriented:

- `irods_path` stays in the query string for full iRODS logical paths.
- Generic iRODS operations stay under `/api/v1/path*`, `/api/v1/user*`, `/api/v1/usergroup*`, `/api/v1/ticket*`, and `/api/v1/server`.
- Workflow-specific APIs stay under `/api/v1/ext/*`.
- Do not introduce parallel external-authorization-source-specific user/group routes when the generic iRODS user and group routes express the operation.

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

- Bearer tokens are validated through the configured external authorization source.
- Basic auth maps directly to iRODS user credentials.
- `Bearer irods-ticket:<ticket>` is accepted only for content download flows.
- `/web/*` browser login uses server-side sessions, but `/api/v1/*` must not depend on web session state.

Bearer token acquisition options targeted for beta:

- Authorization Code + PKCE for browser clients.
- Device Code flow for CLI sign-in.
- Client Credentials for service-to-service automation.
- Token exchange when scoped downstream tokens are required.

Scripted token minting through an authorization source admin API is a development convenience, not production UX.

## User And Group Sync

There are two intentionally different ways to administer users and groups.

Normal user and group routes are direct iRODS administration:

- `POST /api/v1/user`
- `PUT /api/v1/user/{user_name}/type`
- `PUT /api/v1/user/{user_name}/password`
- `PUT /api/v1/user/{user_name}` as a compatibility route for one update field only
- `DELETE /api/v1/user/{user_name}`
- `POST /api/v1/usergroup`
- `DELETE /api/v1/usergroup/{group_name}`
- `POST /api/v1/usergroup/{group_name}/member`
- `DELETE /api/v1/usergroup/{group_name}/member/{user_name}`

Use normal routes when a caller is intentionally making an iRODS catalog change.
The result should reflect the requested operation exactly. For example, creating
an already-existing user is a conflict unless the route documents a specific
idempotent behavior.

`reconcile=true` changes the meaning of the same user and group mutation routes.
It is for desired-state sync from an external authorization source. The caller is
not saying "create this row now" as much as "make iRODS match this external
source record." That means reconcile calls may treat already-correct state as
success and may enforce sync ownership rules before changing or deleting a
principal.

REST does not maintain a separate sync ACL or service-account state store.
Service-account bearer callers follow the same path as other bearer callers:
validate token, resolve the effective/proxy iRODS account, then let the iRODS
user type determine whether catalog mutation can proceed.

Keep desired-state sync policy in `go-irodsclient-extensions/usersync` where possible:

- sync assumes an already-authorized iRODS filesystem
- sync may manage normal `rodsuser` accounts and `rodsgroup` groups
- sync must not manage `groupadmin` or `rodsadmin` users
- iRODS privilege changes stay outside sync and should be handled through iRODS admin workflows

Durable sync ownership state is recorded as iRODS-native AVUs on the user or group:

- `iRODS:USER_SYNCH:MANAGED`
- `iRODS:USER_SYNCH:SOURCE`
- `iRODS:USER_SYNCH:REALM`
- `iRODS:USER_SYNCH:EXTERNAL_ID`
- `iRODS:USER_SYNCH:LAST_SYNC_AT`
- `iRODS:USER_SYNCH:LAST_PLAN_ID`

Those AVUs are the guardrail that lets reconcile delete or revise only the
principals the sync process owns. A normal admin-created user or group should not
be removed by reconcile unless it has been marked as sync-managed.

### Users And Groups Administration

Generic iRODS user and usergroup APIs :

- `GET /api/v1/user`
- `POST /api/v1/user`
- `GET /api/v1/user/{user_name}`
- `PUT /api/v1/user/{user_name}/type`
- `PUT /api/v1/user/{user_name}/password`
- `PUT /api/v1/user/{user_name}` as a compatibility route for one update field only
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

These are iRODS administration operations, not external-source sync operations.
Use `reconcile=true` only for flows that are explicitly syncing desired state
from an external authorization source.

Keep user type and password updates separate. A type change is a privilege
change, while a password change is a credential change. New clients should use
the explicit `/type` and `/password` routes. The compatibility `PUT
/api/v1/user/{user_name}` route must reject requests that include both fields.

Match the iRODS `igroupadmin` privilege boundary for `groupadmin` callers:

- may create `rodsuser` users and set an initial password at creation time
- may create groups
- may add users to groups and remove users from groups
- may not delete users
- may not delete groups
- may not change user type, including self-promotion to `rodsadmin` or self-demotion to `rodsuser`
- may not create `groupadmin` or `rodsadmin` users
- may not use user reconcile/sync operations

The groupadmin `rodsuser` create path should call
`go-irodsclient-extensions/usersandgroups` so it matches `igroupadmin mkuser`.
Do not route that case through the generic go-irodsclient user create primitive;
that path is rodsadmin-oriented and does not model the groupadmin command.

Use the efficient summary/search routes for list-scale views:

- `GET /api/v1/usergroup/summary`
- `GET /api/v1/user/membership-summary`
- `GET /api/v1/user/{user_name}/usergroup`
- `GET /api/v1/principal`
- `GET /api/v1/user/me`

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
