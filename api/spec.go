package api

import _ "embed"

// OpenAPISpec contains the bundled OpenAPI specification served by the app.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
