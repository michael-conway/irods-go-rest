package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/irods"
)

func TestHealthz(t *testing.T) {
	handler := NewHandler(config.FromEnv(), irods.NewCatalogService(config.FromEnv()))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	handler.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}
