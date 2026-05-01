//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestFavoritesLifecycleBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	firstFavoriteName := "e2e-favorite-file-" + randomToken(nil, 8)
	secondFavoriteName := "e2e-favorite-collection-" + randomToken(nil, 8)
	renamedFavoriteName := "e2e-favorite-file-renamed-" + randomToken(nil, 8)

	firstCreateStatus, firstCreateBody := requestFavoritesE2E(
		t, client, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/ext/favorites",
		map[string]any{
			"name":          firstFavoriteName,
			"absolute_path": fixture.objectPath,
		},
	)
	if firstCreateStatus != http.StatusCreated {
		t.Fatalf("expected 201 creating first favorite, got %d: %s", firstCreateStatus, strings.TrimSpace(firstCreateBody))
	}

	secondCreateStatus, secondCreateBody := requestFavoritesE2E(
		t, client, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/ext/favorites",
		map[string]any{
			"name":          secondFavoriteName,
			"absolute_path": fixture.collectionPath,
		},
	)
	if secondCreateStatus != http.StatusCreated {
		t.Fatalf("expected 201 creating second favorite, got %d: %s", secondCreateStatus, strings.TrimSpace(secondCreateBody))
	}

	listStatus, listBody := requestFavoritesE2E(
		t, client, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/ext/favorites", nil,
	)
	if listStatus != http.StatusOK {
		t.Fatalf("expected 200 listing favorites, got %d: %s", listStatus, strings.TrimSpace(listBody))
	}
	assertFavoritePresentE2E(t, listBody, fixture.objectPath, firstFavoriteName)
	assertFavoritePresentE2E(t, listBody, fixture.collectionPath, secondFavoriteName)

	renameStatus, renameBody := requestFavoritesE2E(
		t, client, http.MethodPut, strings.TrimRight(baseURL, "/")+"/api/v1/ext/favorites",
		map[string]any{
			"name":          renamedFavoriteName,
			"absolute_path": fixture.objectPath,
		},
	)
	if renameStatus != http.StatusOK {
		t.Fatalf("expected 200 renaming favorite, got %d: %s", renameStatus, strings.TrimSpace(renameBody))
	}

	filteredStatus, filteredBody := requestFavoritesE2E(
		t, client, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/ext/favorites?absolute_path="+url.QueryEscape(fixture.objectPath), nil,
	)
	if filteredStatus != http.StatusOK {
		t.Fatalf("expected 200 filtering favorites, got %d: %s", filteredStatus, strings.TrimSpace(filteredBody))
	}
	assertFavoritePresentE2E(t, filteredBody, fixture.objectPath, renamedFavoriteName)

	deleteCollectionStatus, deleteCollectionBody := requestFavoritesE2E(
		t, client, http.MethodDelete, strings.TrimRight(baseURL, "/")+"/api/v1/ext/favorites",
		map[string]any{
			"absolute_path": fixture.collectionPath,
		},
	)
	if deleteCollectionStatus != http.StatusNoContent {
		t.Fatalf("expected 204 deleting collection favorite, got %d: %s", deleteCollectionStatus, strings.TrimSpace(deleteCollectionBody))
	}

	deleteFileStatus, deleteFileBody := requestFavoritesE2E(
		t, client, http.MethodDelete, strings.TrimRight(baseURL, "/")+"/api/v1/ext/favorites",
		map[string]any{
			"absolute_path": fixture.objectPath,
		},
	)
	if deleteFileStatus != http.StatusNoContent {
		t.Fatalf("expected 204 deleting file favorite, got %d: %s", deleteFileStatus, strings.TrimSpace(deleteFileBody))
	}
}

func requestFavoritesE2E(t *testing.T, client *http.Client, method string, requestURL string, payload any) (int, string) {
	t.Helper()

	var bodyReader io.Reader
	if payload != nil {
		bodyBytes, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal %s request payload: %v", method, err)
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
		t.Fatalf("read %s response body: %v", method, err)
	}

	return resp.StatusCode, string(body)
}

func assertFavoritePresentE2E(t *testing.T, responseBody string, expectedPath string, expectedName string) {
	t.Helper()

	var payload struct {
		Favorites []struct {
			Name         string `json:"name"`
			AbsolutePath string `json:"absolute_path"`
		} `json:"favorites"`
	}
	if err := json.Unmarshal([]byte(responseBody), &payload); err != nil {
		t.Fatalf("decode favorites response: %v", err)
	}

	for _, favorite := range payload.Favorites {
		if strings.TrimSpace(favorite.AbsolutePath) != expectedPath {
			continue
		}
		if strings.TrimSpace(favorite.Name) != expectedName {
			t.Fatalf("expected favorite %q name %q, got %q", expectedPath, expectedName, favorite.Name)
		}
		return
	}

	t.Fatalf("expected favorite path %q in response: %s", expectedPath, responseBody)
}
