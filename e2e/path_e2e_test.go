//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestHealthzE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, baseURL+"/healthz", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestPathRequiresAuthenticationE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", fixture.collectionPath), nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestGetCollectionPathBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", fixture.collectionPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload struct {
		Path        string `json:"path"`
		Kind        string `json:"kind"`
		HasChildren bool   `json:"hasChildren"`
		ChildCount  int    `json:"childCount"`
	}
	decodeJSON(t, resp.Body, &payload)

	if payload.Path != fixture.collectionPath {
		t.Fatalf("expected path %q, got %q", fixture.collectionPath, payload.Path)
	}
	if payload.Kind != "collection" {
		t.Fatalf("expected kind collection, got %q", payload.Kind)
	}
	if !payload.HasChildren {
		t.Fatal("expected collection to have children")
	}
	if payload.ChildCount < 1 {
		t.Fatalf("expected childCount >= 1, got %d", payload.ChildCount)
	}
}

func TestGetObjectPathBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload struct {
		Path     string `json:"path"`
		Kind     string `json:"kind"`
		Size     int64  `json:"size"`
		Resource string `json:"resource"`
	}
	decodeJSON(t, resp.Body, &payload)

	if payload.Path != fixture.objectPath {
		t.Fatalf("expected path %q, got %q", fixture.objectPath, payload.Path)
	}
	if payload.Kind != "data_object" {
		t.Fatalf("expected kind data_object, got %q", payload.Kind)
	}
	if payload.Size <= 0 {
		t.Fatalf("expected positive size, got %d", payload.Size)
	}
	if strings.TrimSpace(payload.Resource) == "" {
		t.Fatal("expected resource to be populated")
	}
}

func TestGetObjectPathVerboseLongBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURLWithQuery(baseURL, "/api/v1/path", fixture.objectPath, "verbose=1"), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload struct {
		Path     string `json:"path"`
		Replicas []struct {
			Number            int64  `json:"number"`
			ResourceName      string `json:"resource_name"`
			ResourceHierarchy string `json:"resource_hierarchy"`
			Status            string `json:"status"`
			StatusSymbol      string `json:"status_symbol"`
			StatusDescription string `json:"status_description"`
		} `json:"replicas"`
	}
	decodeJSON(t, resp.Body, &payload)

	if payload.Path != fixture.objectPath {
		t.Fatalf("expected path %q, got %q", fixture.objectPath, payload.Path)
	}
	if len(payload.Replicas) < 1 {
		t.Fatal("expected at least one replica in verbose=1 response")
	}
	if strings.TrimSpace(payload.Replicas[0].ResourceName) == "" {
		t.Fatal("expected resource_name in verbose=1 response")
	}
	if strings.TrimSpace(payload.Replicas[0].ResourceHierarchy) == "" {
		t.Fatal("expected resource_hierarchy in verbose=1 response")
	}
	if strings.TrimSpace(payload.Replicas[0].Status) == "" || strings.TrimSpace(payload.Replicas[0].StatusSymbol) == "" {
		t.Fatal("expected replica status fields in verbose=1 response")
	}
}

func TestGetObjectPathVerboseVeryLongBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURLWithQuery(baseURL, "/api/v1/path", fixture.objectPath, "verbose=2"), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload struct {
		Path     string `json:"path"`
		Replicas []struct {
			Checksum     string `json:"checksum"`
			DataType     string `json:"data_type"`
			PhysicalPath string `json:"physical_path"`
		} `json:"replicas"`
	}
	decodeJSON(t, resp.Body, &payload)

	if payload.Path != fixture.objectPath {
		t.Fatalf("expected path %q, got %q", fixture.objectPath, payload.Path)
	}
	if len(payload.Replicas) < 1 {
		t.Fatal("expected at least one replica in verbose=2 response")
	}
	if strings.TrimSpace(payload.Replicas[0].Checksum) == "" {
		t.Fatal("expected checksum in verbose=2 response")
	}
	if strings.TrimSpace(payload.Replicas[0].DataType) == "" {
		t.Fatal("expected data_type in verbose=2 response")
	}
	if strings.TrimSpace(payload.Replicas[0].PhysicalPath) == "" {
		t.Fatal("expected physical_path in verbose=2 response")
	}
}

func TestGetPathChildrenBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/children", fixture.collectionPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload struct {
		IRODSPath string `json:"irods_path"`
		Children  []struct {
			Path string `json:"path"`
			Kind string `json:"kind"`
		} `json:"children"`
	}
	decodeJSON(t, resp.Body, &payload)

	if payload.IRODSPath != fixture.collectionPath {
		t.Fatalf("expected irods_path %q, got %q", fixture.collectionPath, payload.IRODSPath)
	}
	if len(payload.Children) < 1 {
		t.Fatal("expected at least one child entry")
	}

	foundExpectedCollection := false
	for _, child := range payload.Children {
		if child.Path == fixture.childCollectionPath && child.Kind == "collection" {
			foundExpectedCollection = true
			break
		}
	}

	if !foundExpectedCollection {
		t.Fatalf("expected child collection %q in response", fixture.childCollectionPath)
	}
}

func TestGetPathAVUsBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/avu", fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload struct {
		IRODSPath string `json:"irods_path"`
		AVUs      []struct {
			ID        string `json:"id"`
			Attrib    string `json:"attrib"`
			Value     string `json:"value"`
			Unit      string `json:"unit"`
			CreatedAt string `json:"created_at"`
			UpdatedAt string `json:"updated_at"`
		} `json:"avus"`
	}
	decodeJSON(t, resp.Body, &payload)

	if payload.IRODSPath != fixture.objectPath {
		t.Fatalf("expected irods_path %q, got %q", fixture.objectPath, payload.IRODSPath)
	}
	if len(payload.AVUs) < 1 {
		t.Fatal("expected at least one AVU in response")
	}

	foundExpectedAVU := false
	for _, avu := range payload.AVUs {
		if avu.Attrib == fixture.objectAVU.Attrib && avu.Value == fixture.objectAVU.Value && avu.Unit == fixture.objectAVU.Unit {
			foundExpectedAVU = true
			if strings.TrimSpace(avu.ID) == "" {
				t.Fatal("expected AVU id to be populated")
			}
			if strings.TrimSpace(avu.CreatedAt) == "" || strings.TrimSpace(avu.UpdatedAt) == "" {
				t.Fatal("expected AVU timestamps to be populated")
			}
			break
		}
	}

	if !foundExpectedAVU {
		t.Fatalf("expected AVU %q=%q [%q] in response", fixture.objectAVU.Attrib, fixture.objectAVU.Value, fixture.objectAVU.Unit)
	}
}

func TestGetMissingPathBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", fixture.missingPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestGetPathContentsRangeBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/contents", fixture.objectPath), nil)
	req.Header.Set("Range", "bytes=0-15")
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("expected Accept-Ranges bytes, got %q", got)
	}
	if got := resp.Header.Get("Content-Range"); !strings.HasPrefix(got, "bytes 0-15/") {
		t.Fatalf("expected Content-Range prefix %q, got %q", "bytes 0-15/", got)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(body) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(body))
	}
}

func TestGetPathWithBearerTokenE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	token := requireE2EBearerToken(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", fixture.objectPath), nil)
	setBearerAuth(req, token)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func pathURL(baseURL string, route string, irodsPath string) string {
	return strings.TrimRight(baseURL, "/") + route + "?irods_path=" + url.QueryEscape(irodsPath)
}

func pathURLWithQuery(baseURL string, route string, irodsPath string, extraQuery string) string {
	url := pathURL(baseURL, route, irodsPath)
	if strings.TrimSpace(extraQuery) == "" {
		return url
	}

	return url + "&" + extraQuery
}

func decodeJSON(t *testing.T, body io.Reader, target any) {
	t.Helper()

	if err := json.NewDecoder(body).Decode(target); err != nil {
		t.Fatalf("decode json: %v", err)
	}
}
