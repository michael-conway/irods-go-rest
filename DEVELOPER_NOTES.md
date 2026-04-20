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