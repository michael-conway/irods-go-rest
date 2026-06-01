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

func TestSyncUserGroupReconciliationLifecycleE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	userName := "e2esyncu" + randomToken(nil, 10)
	groupName := "e2esyncg" + randomToken(nil, 10)
	zone := e2eIRODSZone(t)

	cleanupSyncSubjectsE2E(t, client, baseURL, userName, groupName)
	t.Cleanup(func() {
		cleanupSyncSubjectsE2E(t, client, baseURL, userName, groupName)
	})

	userEndpoint := syncE2EURL(baseURL, "/api/v1/user/"+url.PathEscape(userName), syncE2EQuery(t, true))
	userCollectionEndpoint := syncE2EURL(baseURL, "/api/v1/user", syncE2EQuery(t, true))
	groupEndpoint := syncE2EURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName), syncE2EQuery(t, true))
	groupCollectionEndpoint := syncE2EURL(baseURL, "/api/v1/usergroup", syncE2EQuery(t, true))
	memberCollectionEndpoint := syncE2EURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member", syncE2EQuery(t, true))
	memberEndpoint := syncE2EURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member/"+url.PathEscape(userName), syncE2EQuery(t, true))

	status, body := requestSyncAdminE2E(t, client, http.MethodPut, userEndpoint, map[string]any{
		"type": "rodsuser",
	})
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncUserE2E(t, body, userName, zone, "rodsuser")

	status, body = requestSyncAdminE2E(t, client, http.MethodPut, userEndpoint, map[string]any{
		"type": "rodsuser",
	})
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncUserE2E(t, body, userName, zone, "rodsuser")

	status, body = requestSyncAdminE2E(t, client, http.MethodPost, userCollectionEndpoint, map[string]any{
		"name": userName,
		"type": "rodsuser",
	})
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncUserE2E(t, body, userName, zone, "rodsuser")

	status, body = requestSyncAdminE2E(t, client, http.MethodPost, userCollectionEndpoint, map[string]any{
		"name": userName,
		"type": "rodsadmin",
	})
	requireSyncStatusE2E(t, status, body, http.StatusConflict)

	status, body = requestSyncAdminE2E(t, client, http.MethodPost, groupCollectionEndpoint, map[string]any{
		"name": groupName,
	})
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncGroupE2E(t, body, groupName, zone)

	status, body = requestSyncAdminE2E(t, client, http.MethodPost, groupCollectionEndpoint, map[string]any{
		"name": groupName,
	})
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncGroupE2E(t, body, groupName, zone)

	status, body = requestSyncAdminE2E(t, client, http.MethodPost, memberCollectionEndpoint, map[string]any{
		"user_name": userName,
	})
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncGroupMemberE2E(t, body, groupName, userName, true)

	status, body = requestSyncAdminE2E(t, client, http.MethodPost, memberCollectionEndpoint, map[string]any{
		"user_name": userName,
	})
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncGroupMemberE2E(t, body, groupName, userName, true)

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, memberEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncGroupMemberE2E(t, body, groupName, userName, false)

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, memberEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusOK)
	assertSyncGroupMemberE2E(t, body, groupName, userName, false)

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, userEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusNoContent)

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, userEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusNoContent)

	status, body = requestSyncAdminE2E(t, client, http.MethodGet, userEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusNotFound)

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, groupEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusNoContent)

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, groupEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusNoContent)

	status, body = requestSyncAdminE2E(t, client, http.MethodGet, groupEndpoint, nil)
	requireSyncStatusE2E(t, status, body, http.StatusNotFound)
}

func cleanupSyncSubjectsE2E(t *testing.T, client *http.Client, baseURL string, userName string, groupName string) {
	t.Helper()

	memberEndpoint := syncE2EURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName)+"/member/"+url.PathEscape(userName), syncE2EQuery(t, true))
	userEndpoint := syncE2EURL(baseURL, "/api/v1/user/"+url.PathEscape(userName), syncE2EQuery(t, true))
	groupEndpoint := syncE2EURL(baseURL, "/api/v1/usergroup/"+url.PathEscape(groupName), syncE2EQuery(t, true))

	status, body := requestSyncAdminE2E(t, client, http.MethodDelete, memberEndpoint, nil)
	if status != http.StatusOK && status != http.StatusNotFound {
		t.Logf("sync e2e cleanup remove member got %d: %s", status, strings.TrimSpace(body))
	}

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, userEndpoint, nil)
	if status != http.StatusNoContent {
		t.Logf("sync e2e cleanup delete user got %d: %s", status, strings.TrimSpace(body))
	}

	status, body = requestSyncAdminE2E(t, client, http.MethodDelete, groupEndpoint, nil)
	if status != http.StatusNoContent {
		t.Logf("sync e2e cleanup delete group got %d: %s", status, strings.TrimSpace(body))
	}
}

func requestSyncAdminE2E(t *testing.T, client *http.Client, method string, requestURL string, payload any) (int, string) {
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
	req.Header.Set("X-Request-ID", "e2e-sync-"+randomToken(nil, 12))
	req.Header.Set("X-IRODS-Source", "irods-go-rest-e2e")
	req.Header.Set("X-IRODS-Actor", "sync-reconcile")
	req.Header.Set("Idempotency-Key", "e2e-sync-"+randomToken(nil, 16))
	setBasicAuthCredentials(req, e2eIRODSUser(t), e2eIRODSPassword(t))

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

func syncE2EURL(baseURL string, path string, query url.Values) string {
	requestURL := strings.TrimRight(baseURL, "/") + path
	if encoded := query.Encode(); encoded != "" {
		requestURL += "?" + encoded
	}
	return requestURL
}

func syncE2EQuery(t *testing.T, reconcile bool) url.Values {
	t.Helper()

	query := url.Values{}
	query.Set("zone", e2eIRODSZone(t))
	if reconcile {
		query.Set("reconcile", "true")
	}
	return query
}

func requireSyncStatusE2E(t *testing.T, got int, body string, expected ...int) {
	t.Helper()

	for _, status := range expected {
		if got == status {
			return
		}
	}
	t.Fatalf("expected status %v, got %d: %s", expected, got, strings.TrimSpace(body))
}

func assertSyncUserE2E(t *testing.T, responseBody string, expectedName string, expectedZone string, expectedType string) {
	t.Helper()

	var payload struct {
		User struct {
			Name string `json:"name"`
			Zone string `json:"zone"`
			Type string `json:"type"`
		} `json:"user"`
	}
	if err := json.Unmarshal([]byte(responseBody), &payload); err != nil {
		t.Fatalf("decode user response: %v: %s", err, strings.TrimSpace(responseBody))
	}
	if payload.User.Name != expectedName || payload.User.Zone != expectedZone || payload.User.Type != expectedType {
		t.Fatalf("expected user %s#%s type %s, got %+v", expectedZone, expectedName, expectedType, payload.User)
	}
}

func assertSyncGroupE2E(t *testing.T, responseBody string, expectedName string, expectedZone string) {
	t.Helper()

	payload := decodeSyncGroupResponseE2E(t, responseBody)
	if payload.Group.Name != expectedName || payload.Group.Zone != expectedZone || payload.Group.Type != "rodsgroup" {
		t.Fatalf("expected group %s#%s type rodsgroup, got %+v", expectedZone, expectedName, payload.Group)
	}
}

func assertSyncGroupMemberE2E(t *testing.T, responseBody string, expectedGroupName string, expectedMemberName string, wantPresent bool) {
	t.Helper()

	payload := decodeSyncGroupResponseE2E(t, responseBody)
	if payload.Group.Name != expectedGroupName {
		t.Fatalf("expected group %q, got %q", expectedGroupName, payload.Group.Name)
	}

	for _, member := range payload.Group.Members {
		if member.Name == expectedMemberName {
			if !wantPresent {
				t.Fatalf("expected member %q to be absent from group %q", expectedMemberName, expectedGroupName)
			}
			return
		}
	}

	if wantPresent {
		t.Fatalf("expected member %q to be present in group %q; members=%+v", expectedMemberName, expectedGroupName, payload.Group.Members)
	}
}

func decodeSyncGroupResponseE2E(t *testing.T, responseBody string) struct {
	Group struct {
		Name    string `json:"name"`
		Zone    string `json:"zone"`
		Type    string `json:"type"`
		Members []struct {
			Name string `json:"name"`
			Zone string `json:"zone"`
			Type string `json:"type"`
		} `json:"members"`
	} `json:"group"`
} {
	t.Helper()

	var payload struct {
		Group struct {
			Name    string `json:"name"`
			Zone    string `json:"zone"`
			Type    string `json:"type"`
			Members []struct {
				Name string `json:"name"`
				Zone string `json:"zone"`
				Type string `json:"type"`
			} `json:"members"`
		} `json:"group"`
	}
	if err := json.Unmarshal([]byte(responseBody), &payload); err != nil {
		t.Fatalf("decode group response: %v: %s", err, strings.TrimSpace(responseBody))
	}
	return payload
}
