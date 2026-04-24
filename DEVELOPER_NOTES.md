## AI Summary

This block is intended as a short operational summary for Codex or another AI assistant working in this repository.

`irods-go-rest` is an OpenAPI-first REST service for iRODS. The core design assumption is that logical iRODS paths are the canonical identifiers for path-oriented operations, so the service uses `/path` as the resource namespace and `irods_path` as a required query parameter rather than embedding full iRODS paths in URL path segments. This is an intentional REST compromise to avoid brittle routing and encoding behavior with `/`, spaces, and other path characters. Within those constraints, the API should remain as RESTful as possible.

Primary API model:

* `GET /api/v1/path?irods_path=...` resolves either a data object or a collection
* the response includes a `kind` discriminator such as `data_object` or `collection`
* collection-specific behavior lives in subresources like `/api/v1/path/children`
* data-object byte streaming lives in `/api/v1/path/contents`
* HATEOAS is a core design principle: responses should expose navigable REST links where practical, starting with `parent` links for path traversal and expanding to sibling/subresource links as the API grows

Architectural assumptions:

* keep HTTP routing, auth, and response mapping in `internal/httpapi/`
* keep iRODS lookup and content retrieval logic behind the `internal/irods` service boundary
* avoid pushing REST URL-building or handler concerns down into the iRODS service layer
* the OpenAPI file in `api/openapi.yaml` is the contract source of truth

Auth assumptions:

* protected endpoints currently accept Bearer and Basic auth
* content endpoints also accept `Bearer irods-ticket:<ticket>` as a scaffold for DRS-style ticket-backed download flows
* browser login remains separate under `/web/*`

Testing and workflow assumptions:

* package-local unit tests should remain next to the code they validate
* docker-compose-backed HTTP end-to-end tests belong under `e2e/`
* `DRS_TEST_BEARER_TOKEN` is intentionally reused as the shared bearer token variable across `irods-go-rest` and `irods-go-drs`

When changing the API, preserve the `/path` model unless there is a strong reason to introduce a distinct identifier space. Prefer one generic path lookup plus type-specific subresources over separate top-level file-versus-collection lookup endpoints. Favor HATEOAS-style links in responses when they help clients navigate hierarchy or discover next operations without reconstructing URLs manually.

I scaffolded irods-go-rest into a clean OpenAPI-first Go service that already builds as a starter API.

The main shape is in place:

OpenAPI contract and codegen config in api/openapi.yaml (line 1) and api/oapi-codegen.yaml (line 1)
App entrypoint and lifecycle in cmd/irods-go-rest/main.go (line 1) and internal/app/app.go (line 1)
HTTP routing and handlers in internal/httpapi/handler.go (line 11) and internal/httpapi/handlers_catalog.go (line 10)
iRODS integration boundary in internal/irods/catalog.go (line 12)
Project guidance in README.md (line 1)
Right now it exposes:

GET /healthz
GET /api/v1/path?irods_path=...
GET /api/v1/path/children?irods_path=...
HEAD /api/v1/path/contents?irods_path=...
GET /api/v1/path/contents?irods_path=...
I kept the current iRODS layer as a stubbed service so the structure is solid before we bind in go-irodsclient. Verification passed with gofmt and go test ./....

### Auth notes

The API is now a bearer-token resource server: protected endpoints are wrapped with Keycloak-backed bearer validation in internal/httpapi/middleware_auth.go (line 11), and the handler wiring now separates API auth from browser auth in internal/httpapi/handler.go (line 13). Keycloak now supports both the browser authorization-code flow and token introspection in internal/auth/keycloak.go (line 64).

The companion web login flow moved under /web: /web/login starts the redirect, /web/callback exchanges and verifies the token, and an HTTP-only session is created in the in-memory store at internal/auth/session_store.go (line 10). Those handlers live in internal/httpapi/handlers_web.go (line 17). I also updated the OpenAPI contract so it describes the API as bearer-protected instead of mixing browser redirects into the API spec in api/openapi.yaml (line 26), and refreshed the usage docs in README.md (line 32).

Verification passed with gofmt and go test ./....

One note: token validation is currently done through Keycloak introspection, which is a solid server-side choice and easy to reason about. If you want, the next improvement would be local JWT validation against Keycloak JWKS for lower latency and less Keycloak round-tripping.

### Path and restart design

For logical-path-oriented API work, the service now treats the full iRODS path as request data rather than embedding it in the URL path. That avoids router ambiguity for `/`, spaces, and other path characters.

This is an intentional REST compromise. In a pure route-parameter design the full resource identity would sit entirely in the URL path, but iRODS logical paths are not route-safe identifiers because they contain `/` semantics and may contain spaces or other characters that require escaping. Using `/path` as the resource namespace and `irods_path` as the required query parameter keeps the API readable, keeps resource operations grouped cleanly, and avoids brittle router or proxy behavior that can occur when full iRODS paths are embedded directly in the route. Given that compromise, the remaining design goal is to keep the API as RESTful as possible through stable resource namespaces, typed subresources, and navigable links in responses.

The design choice is also to treat `path` as the primary REST model for both files and collections. In iRODS, a logical path may resolve to either kind of resource, and clients should not have to know the type before lookup. `/path` therefore returns a generic path representation with a type discriminator, while divergent operations are expressed as subresources:

* `/path/contents` for byte streaming from data objects
* `/path/children` for listing child entries of a collection

This keeps the identifier model uniform while still allowing type-specific behavior where it actually diverges.

To support upward navigation, `/path` responses also expose a lightweight HATEOAS-style `parent` link whenever the resolved path has a parent. The response includes both the parent iRODS path and the REST endpoint for that parent so clients can navigate the hierarchy without reconstructing URLs themselves.

Current shape:

- metadata: `GET /api/v1/path?irods_path=...`
- collection children: `GET /api/v1/path/children?irods_path=...`
- content headers: `HEAD /api/v1/path/contents?irods_path=...`
- content bytes: `GET /api/v1/path/contents?irods_path=...`

The content endpoint supports single-range byte restart via the standard `Range: bytes=start-end` header and can authenticate either a normal OAuth bearer token or a scaffolded bearer token of the form `irods-ticket:<ticket>`.

### Docker test framework

If you are using the Docker compose test framework copied from
`../irods-go-drs/deployments`, the intended layout is a versioned compose stack
under `deployments/docker-test-framework/<irods-version>/` with a `postgres`
service, an `irods-provider` service, and a `keycloak` service. The reference
layout in `../irods-go-drs/deployments/docker-test-framework/5-0/` includes:

- `docker-compose.yml` for the local stack
- `Dockerfile.provider` for the iRODS provider image
- `Dockerfile-keycloak` for the Keycloak image
- `keycloak.env` for Keycloak and imported client settings
- `realm-drs.json` for the realm, IdP, and OAuth client import
- `init-db.sql` for PostgreSQL bootstrap
- `docker-entrypoint.sh` and `testsetup-consortium.sh` for iRODS setup

The normal flow is:

```bash
cd deployments/docker-test-framework/5-0
docker compose build
docker compose up
```

That stack is expected to expose:

- PostgreSQL for ICAT and Keycloak backing storage
- iRODS provider on `1247`, `1248`, and `20000-20199`
- Keycloak on `8443`

Once the compose framework is up, `irods-go-rest` can be added as another
service in the same compose network with these baseline settings:

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

Adjust the published host port and OIDC client settings to match your local
realm import and whichever external browser URL should reach `/web/login`.

### Runtime config in Docker

`irods-go-rest` reads `rest-config.yaml` plus `GOREST_*` environment variables,
with environment variables taking precedence over file values. For Docker-based
local development the most relevant variables are:

- `GOREST_PUBLIC_URL` for the externally visible service URL
- `GOREST_REST_LOG_LEVEL` for log verbosity
- `GOREST_IRODS_ZONE` for the target zone, for example `tempZone`
- `GOREST_IRODS_HOST` for the provider hostname, typically `irods-provider`
- `GOREST_IRODS_PORT` for the provider port, usually `1247`
- `GOREST_IRODS_ADMIN_USER` when admin-backed access is needed
- `GOREST_IRODS_ADMIN_PASSWORD` or `GOREST_IRODS_ADMIN_PASSWORD_FILE`
- `GOREST_IRODS_DEFAULT_RESOURCE` for the default target resource
- `GOREST_OIDC_URL` for the Keycloak base URL
- `GOREST_OIDC_REALM` for the realm name, typically `drs`
- `GOREST_OIDC_CLIENT_ID` for the configured Keycloak client
- `GOREST_OIDC_CLIENT_SECRET` or `GOREST_OIDC_CLIENT_SECRET_FILE`
- `GOREST_OIDC_SCOPE` which defaults to `openid profile email`
- `GOREST_OIDC_INSECURE_SKIP_VERIFY=true` only for self-signed local TLS setups

If you want to pin one exact config file in a container, set:

```bash
IRODS_REST_CONFIG_FILE=/config/rest-config.yaml
```

For containerized or production-style deployments, prefer mounted secret files
instead of inline secrets:

```bash
GOREST_IRODS_ADMIN_PASSWORD_FILE=/run/secrets/irods_admin_password
GOREST_OIDC_CLIENT_SECRET_FILE=/run/secrets/oidc_client_secret
```

This keeps non-secret configuration and secrets separate and matches the
intended compose deployment pattern.

### Testing taxonomy

`irods-go-rest` follows the same broad test layering now used in `irods-go-drs`:

* package-local unit tests stay next to the code they validate and run with the normal `go test ./...` flow
* docker-compose-backed HTTP system tests belong under `e2e/`

End-to-end tests should use the `e2e` build tag:

```go
//go:build e2e
// +build e2e
```

Run them explicitly:

```bash
go test -tags=e2e ./e2e/...
```

Current E2E environment conventions:

* `GOREST_E2E_BASE_URL`
* `DRS_TEST_BEARER_TOKEN`
* `GOREST_E2E_SKIP_TLS_VERIFY`

The shared bearer token variable intentionally matches the convention already used across `irods-go-drs` integration and e2e tests so the two services can be exercised in one local test environment without introducing another token variable name.

### Keycloak env file expectations

The reference `keycloak.env` and `realm-drs.json` files in the docker test
framework expect these values to be set before Keycloak starts:

- `GOOGLE_OIDC_CLIENT_ID`
- `GOOGLE_OIDC_CLIENT_SECRET`
- `DRS_API_CLIENT_ID`
- `DRS_API_CLIENT_SECRET`
- `IRODS_REST_WEB_CLIENT_ID`
- `IRODS_REST_WEB_CLIENT_SECRET`

The imported realm config defines two distinct Keycloak clients:

- A bearer-only API client for protected REST endpoints
- A browser-login client for `irods-go-rest` web login and callback handling

The browser-login client must allow the callback URL used by the deployed
service, for example `http://localhost:8080/web/callback`, and the matching web
origin such as `http://localhost:8080`.
