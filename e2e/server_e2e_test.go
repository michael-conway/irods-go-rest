//go:build e2e
// +build e2e

package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestServerInfoRequiresAuthenticationE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/server", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestGetServerInfoBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/v1/server", nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var payload struct {
		ServerInfo struct {
			ReleaseVersion       string   `json:"release_version"`
			APIVersion           string   `json:"api_version"`
			ReconnectPort        int      `json:"reconnect_port"`
			ReconnectAddr        string   `json:"reconnect_addr"`
			Cookie               int      `json:"cookie"`
			IRODSHost            string   `json:"irods_host"`
			IRODSPort            int      `json:"irods_port"`
			IRODSZone            string   `json:"irods_zone"`
			IRODSNegotiation     string   `json:"irods_negotiation"`
			IRODSDefaultResource string   `json:"irods_default_resource"`
			ResourceAffinity     []string `json:"resource_affinity"`
		} `json:"server_info"`
	}
	decodeJSON(t, resp.Body, &payload)

	if strings.TrimSpace(payload.ServerInfo.ReleaseVersion) == "" {
		t.Fatal("expected server_info.release_version to be populated")
	}
	if strings.TrimSpace(payload.ServerInfo.APIVersion) == "" {
		t.Fatal("expected server_info.api_version to be populated")
	}

	if payload.ServerInfo.IRODSHost != e2eIRODSHost(t) {
		t.Fatalf("expected irods_host %q, got %q", e2eIRODSHost(t), payload.ServerInfo.IRODSHost)
	}
	if payload.ServerInfo.IRODSPort != e2eIRODSPort(t) {
		t.Fatalf("expected irods_port %d, got %d", e2eIRODSPort(t), payload.ServerInfo.IRODSPort)
	}
	if payload.ServerInfo.IRODSZone != e2eIRODSZone(t) {
		t.Fatalf("expected irods_zone %q, got %q", e2eIRODSZone(t), payload.ServerInfo.IRODSZone)
	}

	if cfg := optionalE2ERestConfig(t); cfg != nil {
		expectedNegotiation := strings.TrimSpace(cfg.IrodsNegotiationPolicy)
		if expectedNegotiation != "" && payload.ServerInfo.IRODSNegotiation != expectedNegotiation {
			t.Fatalf("expected irods_negotiation %q, got %q", expectedNegotiation, payload.ServerInfo.IRODSNegotiation)
		}

		expectedDefaultResource := strings.TrimSpace(cfg.IrodsDefaultResource)
		// Default resource is optional in server responses and may be blank.
		if expectedDefaultResource != "" && payload.ServerInfo.IRODSDefaultResource != "" && payload.ServerInfo.IRODSDefaultResource != expectedDefaultResource {
			t.Fatalf("expected irods_default_resource %q, got %q", expectedDefaultResource, payload.ServerInfo.IRODSDefaultResource)
		}

		expectedAffinity := make([]string, 0, len(cfg.ResourceAffinity))
		for _, entry := range cfg.ResourceAffinity {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			expectedAffinity = append(expectedAffinity, entry)
		}
		if len(expectedAffinity) > 0 && !equalStringSlices(payload.ServerInfo.ResourceAffinity, expectedAffinity) {
			t.Fatalf("expected resource_affinity %+v, got %+v", expectedAffinity, payload.ServerInfo.ResourceAffinity)
		}
	}
}

func equalStringSlices(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
