//go:build e2e
// +build e2e

package e2e

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestMetadataManifestBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	assertManifestForPath := func(pathValue string, expectedEntryType string, expectedAVU string) {
		t.Helper()

		requestURL := strings.TrimRight(baseURL, "/") + "/api/v1/ext/metadata-manifest?irods_path=" + url.QueryEscape(pathValue)
		req := newE2ERequest(t, http.MethodGet, requestURL, nil)
		setBasicAuth(req)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("perform GET %s: %v", requestURL, err)
		}
		defer resp.Body.Close()

		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read metadata manifest response: %v", err)
		}
		bodyText := string(bodyBytes)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 from metadata manifest endpoint, got %d: %s", resp.StatusCode, strings.TrimSpace(bodyText))
		}

		var manifest struct {
			Schema    string `json:"$schema"`
			Version   string `json:"version"`
			IRODSPath string `json:"irods_path"`
			IRODSURI  string `json:"irods_uri"`
			IRODSHost string `json:"irods_host"`
			IRODSPort int    `json:"irods_port"`
			IRODSZone string `json:"irods_zone"`
			EntryType string `json:"entry_type"`
			Entry     struct {
				Path string `json:"path"`
			} `json:"entry"`
			AVUs []struct {
				Name string `json:"name"`
			} `json:"avus"`
		}
		decodeJSON(t, strings.NewReader(bodyText), &manifest)

		if strings.TrimSpace(manifest.Schema) == "" {
			t.Fatalf("expected schema in metadata manifest response for %q: %s", pathValue, bodyText)
		}
		if strings.TrimSpace(manifest.Version) == "" {
			t.Fatalf("expected version in metadata manifest response for %q: %s", pathValue, bodyText)
		}
		if manifest.IRODSPath != pathValue {
			t.Fatalf("expected irods_path %q, got %q", pathValue, manifest.IRODSPath)
		}
		if manifest.EntryType != expectedEntryType {
			t.Fatalf("expected entry_type %q for %q, got %q", expectedEntryType, pathValue, manifest.EntryType)
		}
		if manifest.Entry.Path != pathValue {
			t.Fatalf("expected entry.path %q, got %q", pathValue, manifest.Entry.Path)
		}

		if !sameHostOrLoopback(manifest.IRODSHost, e2eIRODSHost(t)) {
			t.Fatalf("expected irods_host %q (localhost/loopback equivalent allowed), got %q", e2eIRODSHost(t), manifest.IRODSHost)
		}
		if manifest.IRODSPort != e2eIRODSPort(t) {
			t.Fatalf("expected irods_port %d, got %d", e2eIRODSPort(t), manifest.IRODSPort)
		}
		if manifest.IRODSZone != e2eIRODSZone(t) {
			t.Fatalf("expected irods_zone %q, got %q", e2eIRODSZone(t), manifest.IRODSZone)
		}
		if !strings.Contains(manifest.IRODSURI, pathValue) {
			t.Fatalf("expected irods_uri to include %q, got %q", pathValue, manifest.IRODSURI)
		}

		foundExpectedAVU := false
		for _, avu := range manifest.AVUs {
			if strings.TrimSpace(avu.Name) == expectedAVU {
				foundExpectedAVU = true
				break
			}
		}
		if !foundExpectedAVU {
			t.Fatalf("expected AVU %q in metadata manifest for %q: %s", expectedAVU, pathValue, bodyText)
		}
	}

	assertManifestForPath(fixture.objectPath, "data_object", fixture.objectAVU.Attrib)
	assertManifestForPath(fixture.collectionPath, "collection", fixture.collectionAVU.Attrib)
}
