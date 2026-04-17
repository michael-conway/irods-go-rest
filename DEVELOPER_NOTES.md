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

The next good step is wiring the internal/irods package to the real iRODS client and then switching the HTTP layer to generated oapi-codegen server interfaces if you want the contract to drive handlers directly.