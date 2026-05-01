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

func TestFileCartLifecycleBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	assignedName := "e2e-filecart-" + randomToken(nil, 8)

	createRequestBody := map[string]any{
		"assigned_name": assignedName,
	}
	createResponseBody, createStatus := requestFileCartE2E(t, client, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/ext/filecarts", createRequestBody)
	if createStatus == http.StatusNotFound || createStatus == http.StatusMethodNotAllowed || createStatus == http.StatusNotImplemented {
		t.Skipf("filecart extension endpoint not available yet (status=%d)", createStatus)
	}
	if createStatus != http.StatusCreated {
		t.Fatalf("expected 201 creating file cart, got %d: %s", createStatus, strings.TrimSpace(createResponseBody))
	}

	createdCart := decodeCreatedFileCartE2E(t, createResponseBody)
	if strings.TrimSpace(createdCart.ID) == "" {
		t.Fatalf("expected created cart id in response: %s", createResponseBody)
	}

	addItemRequestBody := map[string]any{
		"path": fixture.objectPath,
		"type": "file",
	}
	addItemURL := strings.TrimRight(baseURL, "/") + "/api/v1/ext/filecarts/" + createdCart.ID + "/entries"
	addItemResponseBody, addItemStatus := requestFileCartE2E(t, client, http.MethodPost, addItemURL, addItemRequestBody)
	if addItemStatus != http.StatusCreated && addItemStatus != http.StatusOK {
		t.Fatalf("expected 200 or 201 adding filecart entry, got %d: %s", addItemStatus, strings.TrimSpace(addItemResponseBody))
	}

	listResponseBody, listStatus := requestFileCartE2E(t, client, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/ext/filecarts", nil)
	if listStatus != http.StatusOK {
		t.Fatalf("expected 200 listing file carts, got %d: %s", listStatus, strings.TrimSpace(listResponseBody))
	}

	listedCarts := decodeListFileCartsE2E(t, listResponseBody)
	if len(listedCarts) < 1 {
		t.Fatalf("expected at least one cart in list response: %s", listResponseBody)
	}

	found := false
	for _, cart := range listedCarts {
		if cart.ID == createdCart.ID {
			found = true
			if strings.TrimSpace(cart.AssignedName) != assignedName {
				t.Fatalf("expected assigned_name %q for created cart, got %q", assignedName, cart.AssignedName)
			}
			break
		}
	}

	if !found {
		t.Fatalf("expected created cart id %q in list response: %s", createdCart.ID, listResponseBody)
	}
}

type fileCartE2E struct {
	ID           string `json:"id"`
	Path         string `json:"path"`
	AssignedName string `json:"assigned_name"`
}

func requestFileCartE2E(t *testing.T, client *http.Client, method string, requestURL string, payload any) (string, int) {
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

	return string(body), resp.StatusCode
}

func decodeCreatedFileCartE2E(t *testing.T, responseBody string) fileCartE2E {
	t.Helper()

	var payload struct {
		ID           string       `json:"id"`
		Path         string       `json:"path"`
		AssignedName string       `json:"assigned_name"`
		Cart         fileCartE2E  `json:"cart"`
		Data         *fileCartE2E `json:"data"`
	}
	if err := json.Unmarshal([]byte(responseBody), &payload); err != nil {
		t.Fatalf("decode create filecart response: %v", err)
	}

	cart := fileCartE2E{
		ID:           strings.TrimSpace(payload.ID),
		Path:         strings.TrimSpace(payload.Path),
		AssignedName: strings.TrimSpace(payload.AssignedName),
	}
	if strings.TrimSpace(cart.ID) != "" {
		return cart
	}
	if strings.TrimSpace(payload.Cart.ID) != "" {
		return payload.Cart
	}
	if payload.Data != nil {
		return *payload.Data
	}

	return cart
}

func decodeListFileCartsE2E(t *testing.T, responseBody string) []fileCartE2E {
	t.Helper()

	var wrapped struct {
		Carts []fileCartE2E `json:"carts"`
		Data  []fileCartE2E `json:"data"`
	}
	if err := json.Unmarshal([]byte(responseBody), &wrapped); err != nil {
		t.Fatalf("decode list filecarts response: %v", err)
	}

	if len(wrapped.Carts) > 0 {
		return wrapped.Carts
	}
	if len(wrapped.Data) > 0 {
		return wrapped.Data
	}

	var direct []fileCartE2E
	if err := json.Unmarshal([]byte(responseBody), &direct); err == nil && len(direct) > 0 {
		return direct
	}

	return []fileCartE2E{}
}
