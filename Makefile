APP := irods-go-rest

.PHONY: run build test fmt generate

run:
	go run ./cmd/$(APP)

build:
	go build ./...

test:
	go test ./...

fmt:
	gofmt -w ./cmd ./internal

generate:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest --config api/oapi-codegen.yaml api/openapi.yaml
