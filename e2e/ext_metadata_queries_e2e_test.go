//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

const savedMetadataQueryDefaultNameE2E = "New Query"

func TestExtMetadataQueriesScopesAndKindsE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	filesystem := newE2EIRODSFilesystem(t)
	t.Cleanup(filesystem.Release)
	fixture := createPathQueryE2EFixture(t, filesystem)

	prefix := "e2e-saved-query-" + randomToken(nil, 8)
	cases := []struct {
		name     string
		query    map[string]any
		expected []string
		absent   []string
	}{
		{
			name: prefix + "-self-collection",
			query: savedMetadataQueryE2EDefinition(
				[]string{"collection"},
				fixture.alpha,
				"self",
				fixture.attrName,
				"frog-coll-alpha",
				"habitat:pond",
			),
			expected: []string{fixture.alpha},
			absent:   []string{fixture.beta, fixture.alphaFile},
		},
		{
			name: prefix + "-children-data-objects",
			query: savedMetadataQueryE2EDefinition(
				[]string{"data_object"},
				fixture.root,
				"children",
				fixture.attrName,
				"frog-file-*",
				"habitat:p%",
			),
			expected: []string{fixture.rootFile},
			absent:   []string{fixture.alphaFile, fixture.deepFile},
		},
		{
			name: prefix + "-descendant-collections",
			query: savedMetadataQueryE2EDefinition(
				[]string{"collection"},
				fixture.alpha,
				"descendants",
				fixture.attrName,
				"frog-coll-*",
				"habitat:p%",
			),
			expected: []string{fixture.beta, fixture.gamma},
			absent:   []string{fixture.alpha, fixture.bravo},
		},
		{
			name: prefix + "-children-mixed",
			query: savedMetadataQueryE2EDefinition(
				[]string{"data_object", "collection"},
				fixture.root,
				"children",
				fixture.attrName,
				"frog-%",
				"habitat:p%",
			),
			expected: []string{fixture.alpha, fixture.rootFile},
			absent:   []string{fixture.alphaFile, fixture.bravoFile},
		},
	}

	created := make([]savedMetadataQueryE2E, 0, len(cases))
	for _, tc := range cases {
		saved := createSavedMetadataQueryE2E(t, client, baseURL, tc.name, "stored "+tc.name, tc.query)
		created = append(created, saved)
		t.Cleanup(func() {
			deleteSavedMetadataQueryE2E(t, client, baseURL, saved.ID)
		})

		readBack := getSavedMetadataQueryE2E(t, client, baseURL, saved.ID)
		if readBack.Name != tc.name {
			t.Fatalf("expected saved query %q name %q, got %q", saved.ID, tc.name, readBack.Name)
		}
		if len(readBack.Query) == 0 {
			t.Fatalf("expected saved query %q to include query JSON", saved.ID)
		}

		result := postPathQueryPayloadE2E(t, client, baseURL, readBack.Query)
		assertPathQueryE2EPathsExactly(t, result.Paths, tc.expected)
		for _, unexpected := range tc.absent {
			assertPathQueryE2EPathAbsent(t, result.Paths, unexpected)
		}
	}

	listed := listSavedMetadataQueriesE2E(t, client, baseURL)
	for _, saved := range created {
		assertSavedMetadataQueryListedE2E(t, listed, saved.ID, saved.Name)
	}
}

func TestExtMetadataQueriesDisplayDefaultsAndDuplicateNamesE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	filesystem := newE2EIRODSFilesystem(t)
	t.Cleanup(filesystem.Release)
	fixture := createPathQueryE2EFixture(t, filesystem)

	query := savedMetadataQueryE2EDefinition(
		[]string{"data_object"},
		fixture.root,
		"children",
		fixture.attrName,
		"frog-*",
		"*",
	)
	status, body := requestSavedMetadataQueryE2E(t, client, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries", map[string]any{
		"query": query,
	})
	if status != http.StatusCreated {
		t.Fatalf("expected 201 creating saved metadata query without display fields, got %d: %s", status, strings.TrimSpace(body))
	}
	defaulted := decodeSavedMetadataQueryE2E(t, body)
	t.Cleanup(func() {
		deleteSavedMetadataQueryE2E(t, client, baseURL, defaulted.ID)
	})
	if defaulted.Name != savedMetadataQueryDefaultNameE2E {
		t.Fatalf("expected default saved metadata query name %q, got %q", savedMetadataQueryDefaultNameE2E, defaulted.Name)
	}
	if defaulted.Description != "" {
		t.Fatalf("expected blank default saved metadata query description, got %q", defaulted.Description)
	}

	updateQuery := savedMetadataQueryE2EDefinition(
		[]string{"collection"},
		fixture.root,
		"children",
		fixture.attrName,
		"frog-coll-*",
		"*",
	)
	status, body = requestSavedMetadataQueryE2E(t, client, http.MethodPut, strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries/"+defaulted.ID, map[string]any{
		"name":        "   ",
		"description": "   ",
		"query":       updateQuery,
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200 updating saved metadata query with blank display fields, got %d: %s", status, strings.TrimSpace(body))
	}
	updated := decodeSavedMetadataQueryE2E(t, body)
	if updated.ID != defaulted.ID {
		t.Fatalf("expected update to preserve saved metadata query id %q, got %q", defaulted.ID, updated.ID)
	}
	if updated.Name != savedMetadataQueryDefaultNameE2E {
		t.Fatalf("expected blank update name to default to %q, got %q", savedMetadataQueryDefaultNameE2E, updated.Name)
	}
	if updated.Description != "" {
		t.Fatalf("expected blank update description, got %q", updated.Description)
	}

	duplicate := createSavedMetadataQueryE2E(t, client, baseURL, savedMetadataQueryDefaultNameE2E, "duplicate display name", query)
	t.Cleanup(func() {
		deleteSavedMetadataQueryE2E(t, client, baseURL, duplicate.ID)
	})
	if duplicate.ID == defaulted.ID {
		t.Fatalf("expected duplicate display-name query to have distinct id %q", duplicate.ID)
	}
	if duplicate.Name != savedMetadataQueryDefaultNameE2E {
		t.Fatalf("expected duplicate display name %q, got %q", savedMetadataQueryDefaultNameE2E, duplicate.Name)
	}

	listed := listSavedMetadataQueriesE2E(t, client, baseURL)
	assertSavedMetadataQueryListedE2E(t, listed, defaulted.ID, savedMetadataQueryDefaultNameE2E)
	assertSavedMetadataQueryListedE2E(t, listed, duplicate.ID, savedMetadataQueryDefaultNameE2E)
}

func TestExtMetadataQueriesRejectsAVUQueryTypeWithConditionsE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	problematicInvocation := map[string]any{
		"name":        "New Query",
		"description": "",
		"query": map[string]any{
			"type":  "avu_query",
			"kinds": []string{"data_object", "collection"},
			"scope": map[string]any{
				"root": "/tempZone/home/test1",
				"mode": "descendants",
			},
			"conditions": []map[string]any{
				{"field": "avu.attrib", "op": "=", "value": "iRODS:DRS:ID"},
				{"field": "avu.value", "op": "like", "value": "%"},
			},
		},
	}

	status, body := requestSavedMetadataQueryE2E(
		t,
		client,
		http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries",
		problematicInvocation,
	)
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for avu_query type with canonical conditions, got %d: %s", status, strings.TrimSpace(body))
	}
	var invalidRequestPayload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	decodeJSON(t, strings.NewReader(body), &invalidRequestPayload)
	if invalidRequestPayload.Code != "invalid_request" {
		t.Fatalf("expected invalid_request code for avu_query type with canonical conditions, got %+v", invalidRequestPayload)
	}
	if invalidRequestPayload.Message != "invalid request" {
		t.Fatalf("expected sanitized invalid_request message, got %+v", invalidRequestPayload)
	}

	canonicalInvocation := map[string]any{
		"name":        "New Query",
		"description": "",
		"query": map[string]any{
			"type":  "entry_query",
			"kinds": []string{"data_object", "collection"},
			"scope": map[string]any{
				"root": "/tempZone/home/test1",
				"mode": "descendants",
			},
			"conditions": []map[string]any{
				{"field": "avu.attrib", "op": "=", "value": "iRODS:DRS:ID"},
				{"field": "avu.value", "op": "like", "value": "%"},
			},
		},
	}
	status, body = requestSavedMetadataQueryE2E(
		t,
		client,
		http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries",
		canonicalInvocation,
	)
	if status != http.StatusCreated {
		t.Fatalf("expected 201 for canonical entry_query conditions payload, got %d: %s", status, strings.TrimSpace(body))
	}
	saved := decodeSavedMetadataQueryE2E(t, body)
	t.Cleanup(func() {
		deleteSavedMetadataQueryE2E(t, client, baseURL, saved.ID)
	})
}

type savedMetadataQueryE2E struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Query       json.RawMessage `json:"query"`
}

type savedMetadataQueryListE2E struct {
	MetadataQueries []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"metadata_queries"`
}

func savedMetadataQueryE2EDefinition(kinds []string, scopeRoot string, scopeMode string, attr string, value string, unit string) map[string]any {
	conditions := []map[string]any{}
	if strings.TrimSpace(attr) != "" && strings.TrimSpace(attr) != "*" && strings.TrimSpace(attr) != "%" {
		conditions = append(conditions, map[string]any{"field": "avu.attrib", "op": "=", "value": attr})
	}
	if strings.TrimSpace(value) != "" && strings.TrimSpace(value) != "*" && strings.TrimSpace(value) != "%" {
		op := "="
		if strings.ContainsAny(value, "*%") {
			op = "like"
		}
		conditions = append(conditions, map[string]any{"field": "avu.value", "op": op, "value": value})
	}
	if strings.TrimSpace(unit) != "" && strings.TrimSpace(unit) != "*" && strings.TrimSpace(unit) != "%" {
		op := "="
		if strings.ContainsAny(unit, "*%") {
			op = "like"
		}
		conditions = append(conditions, map[string]any{"field": "avu.unit", "op": op, "value": unit})
	}

	return map[string]any{
		"type":       "entry_query",
		"kinds":      kinds,
		"conditions": conditions,
		"scope": map[string]any{
			"root": scopeRoot,
			"mode": scopeMode,
		},
		"defaults": map[string]any{
			"limit": 50,
		},
	}
}

func createSavedMetadataQueryE2E(t *testing.T, client *http.Client, baseURL string, name string, description string, query map[string]any) savedMetadataQueryE2E {
	t.Helper()

	status, body := requestSavedMetadataQueryE2E(t, client, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries", map[string]any{
		"name":        name,
		"description": description,
		"query":       query,
	})
	if status != http.StatusCreated {
		t.Fatalf("expected 201 creating saved metadata query, got %d: %s", status, strings.TrimSpace(body))
	}

	return decodeSavedMetadataQueryE2E(t, body)
}

func getSavedMetadataQueryE2E(t *testing.T, client *http.Client, baseURL string, id string) savedMetadataQueryE2E {
	t.Helper()

	status, body := requestSavedMetadataQueryE2E(t, client, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries/"+id, nil)
	if status != http.StatusOK {
		t.Fatalf("expected 200 reading saved metadata query %q, got %d: %s", id, status, strings.TrimSpace(body))
	}
	return decodeSavedMetadataQueryE2E(t, body)
}

func listSavedMetadataQueriesE2E(t *testing.T, client *http.Client, baseURL string) savedMetadataQueryListE2E {
	t.Helper()

	status, body := requestSavedMetadataQueryE2E(t, client, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries", nil)
	if status != http.StatusOK {
		t.Fatalf("expected 200 listing saved metadata queries, got %d: %s", status, strings.TrimSpace(body))
	}

	var payload savedMetadataQueryListE2E
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode saved metadata query list: %v", err)
	}
	return payload
}

func deleteSavedMetadataQueryE2E(t *testing.T, client *http.Client, baseURL string, id string) {
	t.Helper()
	if strings.TrimSpace(id) == "" {
		return
	}

	status, body := requestSavedMetadataQueryE2E(t, client, http.MethodDelete, strings.TrimRight(baseURL, "/")+"/api/v1/ext/metadata-queries/"+id, nil)
	if status != http.StatusNoContent && status != http.StatusNotFound {
		t.Fatalf("expected 204 or 404 deleting saved metadata query %q, got %d: %s", id, status, strings.TrimSpace(body))
	}
}

func requestSavedMetadataQueryE2E(t *testing.T, client *http.Client, method string, requestURL string, payload any) (int, string) {
	t.Helper()

	var bodyReader io.Reader
	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %s saved metadata query payload: %v", method, err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	req := newE2ERequest(t, method, requestURL, bodyReader)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform %s %s: %v", method, requestURL, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s saved metadata query response body: %v", method, err)
	}
	return resp.StatusCode, string(body)
}

func decodeSavedMetadataQueryE2E(t *testing.T, body string) savedMetadataQueryE2E {
	t.Helper()

	var payload struct {
		MetadataQuery savedMetadataQueryE2E `json:"metadata_query"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("decode saved metadata query response: %v", err)
	}
	if strings.TrimSpace(payload.MetadataQuery.ID) == "" {
		t.Fatalf("expected saved metadata query id in response: %s", body)
	}
	if strings.TrimSpace(payload.MetadataQuery.Name) == "" {
		t.Fatalf("expected saved metadata query name in response: %s", body)
	}
	return payload.MetadataQuery
}

func assertSavedMetadataQueryListedE2E(t *testing.T, listed savedMetadataQueryListE2E, id string, name string) {
	t.Helper()

	for _, item := range listed.MetadataQueries {
		if item.ID == id {
			if item.Name != name {
				t.Fatalf("expected listed saved metadata query %q name %q, got %q", id, name, item.Name)
			}
			return
		}
	}
	t.Fatalf("expected saved metadata query %q in list %+v", id, listed.MetadataQueries)
}
