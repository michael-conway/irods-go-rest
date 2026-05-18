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
	return map[string]any{
		"type":  "avu_query",
		"kinds": kinds,
		"scope": map[string]any{
			"root": scopeRoot,
			"mode": scopeMode,
		},
		"avu": map[string]any{
			"attrib": attr,
			"value":  value,
			"unit":   unit,
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
