I scaffolded irods-go-rest into a clean OpenAPI-first Go service that already builds as a starter API.

The main shape is in place:

OpenAPI contract and codegen config in api/openapi.yaml (line 1) and api/oapi-codegen.yaml (line 1)
App entrypoint and lifecycle in cmd/irods-go-rest/main.go (line 1) and internal/app/app.go (line 1)
HTTP routing and handlers in internal/httpapi/handler.go (line 11) and internal/httpapi/handlers_catalog.go (line 10)
iRODS integration boundary in internal/irods/catalog.go (line 12)
Project guidance in README.md (line 1)
Right now it exposes:

GET /healthz
GET /api/v1/objects/{object_id}
GET /api/v1/collections/{collection_id}
I kept the current iRODS layer as a stubbed service so the structure is solid before we bind in go-irodsclient. Verification passed with gofmt and go test ./....

### Auth notes

The API is now a bearer-token resource server: protected endpoints are wrapped with Keycloak-backed bearer validation in internal/httpapi/middleware_auth.go (line 11), and the handler wiring now separates API auth from browser auth in internal/httpapi/handler.go (line 13). Keycloak now supports both the browser authorization-code flow and token introspection in internal/auth/keycloak.go (line 64).

The companion web login flow moved under /web: /web/login starts the redirect, /web/callback exchanges and verifies the token, and an HTTP-only session is created in the in-memory store at internal/auth/session_store.go (line 10). Those handlers live in internal/httpapi/handlers_web.go (line 17). I also updated the OpenAPI contract so it describes the API as bearer-protected instead of mixing browser redirects into the API spec in api/openapi.yaml (line 26), and refreshed the usage docs in README.md (line 32).

Verification passed with gofmt and go test ./....

One note: token validation is currently done through Keycloak introspection, which is a solid server-side choice and easy to reason about. If you want, the next improvement would be local JWT validation against Keycloak JWKS for lower latency and less Keycloak round-tripping.

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
