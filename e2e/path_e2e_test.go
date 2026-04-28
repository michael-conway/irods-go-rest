//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	extension_tickets "github.com/michael-conway/go-irodsclient-extensions/tickets"
)

type e2eTicketSummary struct {
	Name string `json:"name"`
	Path string `json:"irods_path"`
}

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

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	bodyText := string(bodyBytes)

	var payload struct {
		Path        string `json:"path"`
		Kind        string `json:"kind"`
		HasChildren bool   `json:"hasChildren"`
		ChildCount  int    `json:"childCount"`
	}
	decodeJSON(t, strings.NewReader(bodyText), &payload)

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
	if !strings.Contains(bodyText, `"avus":{"href":"/api/v1/path/avu?irods_path=`) {
		t.Fatalf("expected AVU HATEOAS link in path response, got %q", bodyText)
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
		MimeType string `json:"mime_type"`
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
	expectedMimeType := mime.TypeByExtension(filepath.Ext(fixture.objectPath))
	if expectedMimeType == "" {
		expectedMimeType = "application/octet-stream"
	}
	if payload.MimeType != expectedMimeType {
		t.Fatalf("expected mime type %q, got %q", expectedMimeType, payload.MimeType)
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
	checksummedObjectPath := requireE2EChecksummedObjectPath(t, fixture)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURLWithQuery(baseURL, "/api/v1/path", checksummedObjectPath, "verbose=2"), nil)
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

	if payload.Path != checksummedObjectPath {
		t.Fatalf("expected path %q, got %q", checksummedObjectPath, payload.Path)
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

func TestPathFileCreateRenameDeleteBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	parentPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-path-file-"+randomToken(nil, 8),
	)
	if err := filesystem.MakeDir(parentPath, true); err != nil {
		t.Fatalf("make parent collection %q: %v", parentPath, err)
	}
	defer func() {
		if err := filesystem.RemoveDir(parentPath, true, true); err != nil && filesystem.Exists(parentPath) {
			t.Errorf("cleanup parent collection %q: %v", parentPath, err)
		}
	}()

	createReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path", parentPath), strings.NewReader(`{"child_name":"new-file.txt","kind":"data_object"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createReq)

	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("perform create file request: %v", err)
	}
	defer createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201, got %d: %s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var created struct {
		Path        string `json:"path"`
		Kind        string `json:"kind"`
		DisplaySize string `json:"display_size"`
	}
	decodeJSON(t, createResp.Body, &created)

	createdPath := parentPath + "/new-file.txt"
	if created.Path != createdPath {
		t.Fatalf("expected path %q, got %q", createdPath, created.Path)
	}
	if created.Kind != "data_object" {
		t.Fatalf("expected data_object kind, got %q", created.Kind)
	}
	if created.DisplaySize != "0 B" {
		t.Fatalf("expected zero-byte display size, got %q", created.DisplaySize)
	}
	if !waitForIRODSPathFresh(t, createdPath, 3*time.Second) {
		t.Fatalf("expected created file %q to exist", createdPath)
	}

	moveReq := newE2ERequest(t, http.MethodPatch, pathURL(baseURL, "/api/v1/path", createdPath), strings.NewReader(`{"new_name":"renamed-file.txt"}`))
	moveReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(moveReq)

	moveResp, err := client.Do(moveReq)
	if err != nil {
		t.Fatalf("perform rename file request: %v", err)
	}
	defer moveResp.Body.Close()

	if moveResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(moveResp.Body)
		t.Fatalf("expected 200, got %d: %s", moveResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var renamed struct {
		Path string `json:"path"`
		Kind string `json:"kind"`
	}
	decodeJSON(t, moveResp.Body, &renamed)

	renamedPath := parentPath + "/renamed-file.txt"
	if renamed.Path != renamedPath {
		t.Fatalf("expected renamed path %q, got %q", renamedPath, renamed.Path)
	}
	if renamed.Kind != "data_object" {
		t.Fatalf("expected data_object kind after rename, got %q", renamed.Kind)
	}
	if !waitForIRODSPathFresh(t, renamedPath, 3*time.Second) {
		t.Fatalf("expected renamed file %q to exist", renamedPath)
	}
	if waitForIRODSPathFresh(t, createdPath, 500*time.Millisecond) {
		t.Fatalf("expected original file %q to be absent after rename", createdPath)
	}

	deleteReq := newE2ERequest(t, http.MethodDelete, pathURL(baseURL, "/api/v1/path", renamedPath), nil)
	setBasicAuth(deleteReq)

	deleteResp, err := client.Do(deleteReq)
	if err != nil {
		t.Fatalf("perform delete file request: %v", err)
	}
	defer deleteResp.Body.Close()

	if deleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleteResp.Body)
		t.Fatalf("expected 204, got %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
	}
	if waitForIRODSPathFresh(t, renamedPath, 500*time.Millisecond) {
		t.Fatalf("expected file %q to be deleted", renamedPath)
	}
}

func TestPathCollectionCreateDeleteForceBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	parentPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-path-collection-"+randomToken(nil, 8),
	)
	if err := filesystem.MakeDir(parentPath, true); err != nil {
		t.Fatalf("make parent collection %q: %v", parentPath, err)
	}
	defer func() {
		if err := filesystem.RemoveDir(parentPath, true, true); err != nil && filesystem.Exists(parentPath) {
			t.Errorf("cleanup parent collection %q: %v", parentPath, err)
		}
	}()

	createCollectionReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path", parentPath), strings.NewReader(`{"child_name":"child-collection","kind":"collection"}`))
	createCollectionReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createCollectionReq)

	createCollectionResp, err := client.Do(createCollectionReq)
	if err != nil {
		t.Fatalf("perform create collection request: %v", err)
	}
	defer createCollectionResp.Body.Close()

	if createCollectionResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createCollectionResp.Body)
		t.Fatalf("expected 201, got %d: %s", createCollectionResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var createdCollection struct {
		Path string `json:"path"`
		Kind string `json:"kind"`
	}
	decodeJSON(t, createCollectionResp.Body, &createdCollection)

	collectionPath := parentPath + "/child-collection"
	if createdCollection.Path != collectionPath || createdCollection.Kind != "collection" {
		t.Fatalf("unexpected created collection payload %+v", createdCollection)
	}
	if !waitForIRODSPathFresh(t, collectionPath, 3*time.Second) {
		t.Fatalf("expected collection %q to exist", collectionPath)
	}

	createChildFileReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path", collectionPath), strings.NewReader(`{"child_name":"child.txt","kind":"data_object"}`))
	createChildFileReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createChildFileReq)

	createChildFileResp, err := client.Do(createChildFileReq)
	if err != nil {
		t.Fatalf("perform create child file request: %v", err)
	}
	defer createChildFileResp.Body.Close()

	if createChildFileResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createChildFileResp.Body)
		t.Fatalf("expected 201, got %d: %s", createChildFileResp.StatusCode, strings.TrimSpace(string(body)))
	}

	childFilePath := collectionPath + "/child.txt"
	if !waitForIRODSPathFresh(t, childFilePath, 3*time.Second) {
		t.Fatalf("expected child file %q to exist", childFilePath)
	}

	deleteCollectionReq := newE2ERequest(t, http.MethodDelete, pathURL(baseURL, "/api/v1/path", collectionPath), nil)
	setBasicAuth(deleteCollectionReq)

	deleteCollectionResp, err := client.Do(deleteCollectionReq)
	if err != nil {
		t.Fatalf("perform non-force delete collection request: %v", err)
	}
	defer deleteCollectionResp.Body.Close()

	if deleteCollectionResp.StatusCode != http.StatusConflict {
		body, _ := io.ReadAll(deleteCollectionResp.Body)
		t.Fatalf("expected 409, got %d: %s", deleteCollectionResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var conflictPayload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	decodeJSON(t, deleteCollectionResp.Body, &conflictPayload)
	if conflictPayload.Code != "conflict" || !strings.Contains(conflictPayload.Message, "force=true") {
		t.Fatalf("unexpected conflict payload %+v", conflictPayload)
	}
	if !waitForIRODSPathFresh(t, collectionPath, 500*time.Millisecond) {
		t.Fatalf("expected collection %q to still exist after non-force delete", collectionPath)
	}

	forceDeleteReq := newE2ERequest(t, http.MethodDelete, pathURLWithQuery(baseURL, "/api/v1/path", collectionPath, "force=true"), nil)
	setBasicAuth(forceDeleteReq)

	forceDeleteResp, err := client.Do(forceDeleteReq)
	if err != nil {
		t.Fatalf("perform force delete collection request: %v", err)
	}
	defer forceDeleteResp.Body.Close()

	if forceDeleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(forceDeleteResp.Body)
		t.Fatalf("expected 204, got %d: %s", forceDeleteResp.StatusCode, strings.TrimSpace(string(body)))
	}
	if waitForIRODSPathFresh(t, collectionPath, 500*time.Millisecond) {
		t.Fatalf("expected collection %q to be deleted by force", collectionPath)
	}
	if waitForIRODSPathFresh(t, childFilePath, 500*time.Millisecond) {
		t.Fatalf("expected child file %q to be deleted by force", childFilePath)
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
			ID     string `json:"id"`
			Attrib string `json:"attrib"`
			Value  string `json:"value"`
			Unit   string `json:"unit"`
			Links  struct {
				Update struct {
					Href   string `json:"href"`
					Method string `json:"method"`
				} `json:"update"`
				Delete struct {
					Href   string `json:"href"`
					Method string `json:"method"`
				} `json:"delete"`
			} `json:"links"`
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
			if strings.TrimSpace(avu.Links.Update.Href) == "" || strings.TrimSpace(avu.Links.Delete.Href) == "" {
				t.Fatal("expected AVU update/delete links to be populated")
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

func TestPostAndDeletePathAVUBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path/avu", fixture.objectPath), strings.NewReader(`{"attrib":"e2e.added.avu","value":"present","unit":"test"}`))
	req.Header.Set("Content-Type", "application/json")
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform create request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var created struct {
		AVU struct {
			ID     string `json:"id"`
			Attrib string `json:"attrib"`
			Value  string `json:"value"`
			Unit   string `json:"unit"`
		} `json:"avu"`
	}
	decodeJSON(t, resp.Body, &created)

	if strings.TrimSpace(created.AVU.ID) == "" {
		t.Fatal("expected created AVU id to be populated")
	}
	if created.AVU.Attrib != "e2e.added.avu" || created.AVU.Value != "present" || created.AVU.Unit != "test" {
		t.Fatalf("unexpected created AVU payload %+v", created.AVU)
	}

	req = newE2ERequest(t, http.MethodDelete, strings.TrimRight(baseURL, "/")+"/api/v1/path/avu/"+url.PathEscape(created.AVU.ID)+"?irods_path="+url.QueryEscape(fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("perform delete request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	req = newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/avu", fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("perform list request after delete: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var payload struct {
		AVUs []struct {
			ID string `json:"id"`
		} `json:"avus"`
	}
	decodeJSON(t, resp.Body, &payload)

	for _, avu := range payload.AVUs {
		if avu.ID == created.AVU.ID {
			t.Fatalf("expected AVU %q to be removed", created.AVU.ID)
		}
	}
}

func TestDataObjectAVUCreateDeleteReloadBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	filesystem := newE2EIRODSFilesystem(t)
	objectPath := fixture.missingPath + ".avu-roundtrip"
	t.Cleanup(func() {
		defer filesystem.Release()
		if err := filesystem.RemoveFile(objectPath, true); err != nil && filesystem.Exists(objectPath) {
			t.Errorf("cleanup object %q: %v", objectPath, err)
		}
	})

	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBufferString("e2e avu round-trip payload\n"), objectPath, "", false, false, nil); err != nil {
		t.Fatalf("upload object %q: %v", objectPath, err)
	}

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", objectPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform object lookup request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from object lookup, got %d", resp.StatusCode)
	}

	var object struct {
		Path string `json:"path"`
		Kind string `json:"kind"`
	}
	decodeJSON(t, resp.Body, &object)
	if object.Path != objectPath || object.Kind != "data_object" {
		t.Fatalf("unexpected object lookup response %+v", object)
	}

	firstAVU := createPathAVUE2E(t, client, baseURL, objectPath, `{"attrib":"e2e.roundtrip.first","value":"keep","unit":"test"}`)
	secondAVU := createPathAVUE2E(t, client, baseURL, objectPath, `{"attrib":"e2e.roundtrip.second","value":"delete","unit":"test"}`)

	req = newE2ERequest(t, http.MethodDelete, strings.TrimRight(baseURL, "/")+"/api/v1/path/avu/"+url.PathEscape(secondAVU.ID)+"?irods_path="+url.QueryEscape(objectPath), nil)
	setBasicAuth(req)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("perform AVU delete request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 from AVU delete, got %d", resp.StatusCode)
	}

	req = newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/avu", objectPath), nil)
	setBasicAuth(req)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("perform AVU reload request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from AVU reload, got %d", resp.StatusCode)
	}

	var reloaded struct {
		Count int `json:"count"`
		Total int `json:"total"`
		AVUs  []struct {
			ID     string `json:"id"`
			Attrib string `json:"attrib"`
			Value  string `json:"value"`
			Unit   string `json:"unit"`
		} `json:"avus"`
	}
	decodeJSON(t, resp.Body, &reloaded)

	if reloaded.Count != 1 || reloaded.Total != 1 || len(reloaded.AVUs) != 1 {
		t.Fatalf("expected one AVU after reload, got %+v", reloaded)
	}
	if reloaded.AVUs[0].ID != firstAVU.ID || reloaded.AVUs[0].Attrib != firstAVU.Attrib || reloaded.AVUs[0].Value != firstAVU.Value || reloaded.AVUs[0].Unit != firstAVU.Unit {
		t.Fatalf("expected remaining AVU %+v, got %+v", firstAVU, reloaded.AVUs[0])
	}
	if reloaded.AVUs[0].ID == secondAVU.ID {
		t.Fatalf("expected deleted AVU %q to stay removed after reload", secondAVU.ID)
	}
}

func TestPutPathAVUBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path/avu", fixture.objectPath), strings.NewReader(`{"attrib":"e2e.update.avu","value":"before","unit":"test"}`))
	req.Header.Set("Content-Type", "application/json")
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform create request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var created struct {
		AVU struct {
			ID string `json:"id"`
		} `json:"avu"`
	}
	decodeJSON(t, resp.Body, &created)

	req = newE2ERequest(t, http.MethodPut, strings.TrimRight(baseURL, "/")+"/api/v1/path/avu/"+url.PathEscape(created.AVU.ID)+"?irods_path="+url.QueryEscape(fixture.objectPath), strings.NewReader(`{"attrib":"e2e.update.avu","value":"after","unit":"test"}`))
	req.Header.Set("Content-Type", "application/json")
	setBasicAuth(req)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("perform update request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var updated struct {
		AVU struct {
			ID     string `json:"id"`
			Attrib string `json:"attrib"`
			Value  string `json:"value"`
			Unit   string `json:"unit"`
		} `json:"avu"`
	}
	decodeJSON(t, resp.Body, &updated)

	if updated.AVU.Attrib != "e2e.update.avu" || updated.AVU.Value != "after" || updated.AVU.Unit != "test" {
		t.Fatalf("unexpected updated AVU %+v", updated.AVU)
	}
}

func TestPathChecksumBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/checksum", fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var initial struct {
		IRODSPath string `json:"irods_path"`
		Checksum  string `json:"checksum"`
		Type      string `json:"type"`
	}
	decodeJSON(t, resp.Body, &initial)

	if initial.IRODSPath != fixture.objectPath {
		t.Fatalf("expected irods_path %q, got %q", fixture.objectPath, initial.IRODSPath)
	}
	if initial.Checksum != "" || initial.Type != "" {
		t.Fatalf("expected empty checksum before compute, got %+v", initial)
	}

	req = newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path/checksum", fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("perform checksum compute request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from checksum compute, got %d", resp.StatusCode)
	}

	var computed struct {
		Checksum string `json:"checksum"`
		Type     string `json:"type"`
	}
	decodeJSON(t, resp.Body, &computed)

	if strings.TrimSpace(computed.Checksum) == "" {
		t.Fatal("expected computed checksum to be populated")
	}
	if strings.TrimSpace(computed.Type) == "" {
		t.Fatal("expected computed checksum type to be populated")
	}

	req = newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("perform path request after checksum compute: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from path lookup after checksum compute, got %d", resp.StatusCode)
	}

	var pathPayload struct {
		Checksum struct {
			Checksum string `json:"checksum"`
			Type     string `json:"type"`
		} `json:"checksum"`
	}
	decodeJSON(t, resp.Body, &pathPayload)

	if pathPayload.Checksum.Checksum != computed.Checksum {
		t.Fatalf("expected path checksum %q after compute, got %+v", computed.Checksum, pathPayload.Checksum)
	}
	if pathPayload.Checksum.Type != computed.Type {
		t.Fatalf("expected path checksum type %q after compute, got %+v", computed.Type, pathPayload.Checksum)
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
	if got := resp.Header.Get("Content-Disposition"); !strings.Contains(got, `attachment;`) {
		t.Fatalf("expected Content-Disposition attachment header, got %q", got)
	}
	if got := resp.Header.Get("Last-Modified"); got == "" {
		t.Fatal("expected Last-Modified header")
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("expected nosniff header, got %q", got)
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

func TestGetPathContentsIRODSTicketBearerE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	objectPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-ticket-"+randomToken(nil, 8)+".txt",
	)
	content := "irods-go-rest ticket e2e payload\n"

	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBufferString(content), objectPath, "", false, true, nil); err != nil {
		t.Fatalf("upload ticket e2e object %q: %v", objectPath, err)
	}
	defer func() {
		if err := filesystem.RemoveFile(objectPath, true); err != nil && filesystem.Exists(objectPath) {
			t.Errorf("cleanup ticket e2e object %q: %v", objectPath, err)
		}
	}()

	ticketID, bearerToken, err := extension_tickets.CreateAnonymousDataObjectBearerToken(filesystem, objectPath, 5, 30)
	if err != nil {
		t.Fatalf("create anonymous ticket for %q: %v", objectPath, err)
	}
	defer func() {
		if err := filesystem.DeleteTicket(ticketID); err != nil {
			t.Errorf("cleanup ticket %q: %v", ticketID, err)
		}
	}()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/contents", objectPath), nil)
	setBearerAuth(req, bearerToken)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform ticket-auth content request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
		t.Fatalf("expected Accept-Ranges bytes, got %q", got)
	}
	if got := resp.Header.Get("Content-Disposition"); !strings.Contains(got, `attachment;`) {
		t.Fatalf("expected Content-Disposition attachment header, got %q", got)
	}
	if got := resp.Header.Get("Last-Modified"); got == "" {
		t.Fatal("expected Last-Modified header")
	}
	if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("expected nosniff header, got %q", got)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != content {
		t.Fatalf("expected content %q, got %q", content, string(body))
	}
}

func TestGetPathContentsTicketIDQueryE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	objectPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-ticket-query-"+randomToken(nil, 8)+".txt",
	)
	content := "irods-go-rest ticket query payload\n"

	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBufferString(content), objectPath, "", false, true, nil); err != nil {
		t.Fatalf("upload ticket query object %q: %v", objectPath, err)
	}
	defer func() {
		if err := filesystem.RemoveFile(objectPath, true); err != nil && filesystem.Exists(objectPath) {
			t.Errorf("cleanup ticket query object %q: %v", objectPath, err)
		}
	}()

	ticketID, _, err := extension_tickets.CreateAnonymousDataObjectBearerToken(filesystem, objectPath, 5, 30)
	if err != nil {
		t.Fatalf("create anonymous ticket for %q: %v", objectPath, err)
	}
	defer func() {
		if err := filesystem.DeleteTicket(ticketID); err != nil {
			t.Logf("best-effort cleanup ticket %q: %v", ticketID, err)
		}
	}()

	req := newE2ERequest(t, http.MethodGet, pathURLWithTicketID(baseURL, "/api/v1/path/contents", objectPath, ticketID), nil)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform ticket-id content request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != content {
		t.Fatalf("expected content %q, got %q", content, string(body))
	}
}

func TestTicketLifecycleE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	objectPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-rest-ticket-lifecycle-"+randomToken(nil, 8)+".txt",
	)
	content := "irods-go-rest ticket lifecycle payload\n"

	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBufferString(content), objectPath, "", false, true, nil); err != nil {
		t.Fatalf("upload ticket lifecycle object %q: %v", objectPath, err)
	}
	defer func() {
		if err := filesystem.RemoveFile(objectPath, true); err != nil && filesystem.Exists(objectPath) {
			t.Errorf("cleanup ticket lifecycle object %q: %v", objectPath, err)
		}
	}()

	createReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path/ticket", objectPath), strings.NewReader(`{"maximum_uses":5,"lifetime_minutes":30}`))
	createReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createReq)

	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("perform create ticket request: %v", err)
	}
	defer createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201, got %d: %s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var createPayload struct {
		Ticket struct {
			Name        string `json:"name"`
			BearerToken string `json:"bearer_token"`
			Path        string `json:"irods_path"`
			UsesLimit   int64  `json:"uses_limit"`
		} `json:"ticket"`
	}
	if err := json.NewDecoder(createResp.Body).Decode(&createPayload); err != nil {
		t.Fatalf("decode create ticket response: %v", err)
	}

	ticketName := strings.TrimSpace(createPayload.Ticket.Name)
	if ticketName == "" {
		t.Fatal("expected created ticket name")
	}
	if createPayload.Ticket.BearerToken != "irods-ticket:"+ticketName {
		t.Fatalf("expected bearer token for %q, got %q", ticketName, createPayload.Ticket.BearerToken)
	}
	if createPayload.Ticket.Path != objectPath {
		t.Fatalf("expected ticket path %q, got %q", objectPath, createPayload.Ticket.Path)
	}
	if createPayload.Ticket.UsesLimit != 5 {
		t.Fatalf("expected uses limit 5, got %d", createPayload.Ticket.UsesLimit)
	}

	defer func() {
		if err := filesystem.DeleteTicket(ticketName); err != nil {
			t.Logf("best-effort cleanup ticket %q: %v", ticketName, err)
		}
	}()

	listReq := newE2ERequest(t, http.MethodGet, baseURL+"/api/v1/ticket", nil)
	setBasicAuth(listReq)

	listResp, err := client.Do(listReq)
	if err != nil {
		t.Fatalf("perform list tickets request: %v", err)
	}
	defer listResp.Body.Close()

	if listResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(listResp.Body)
		t.Fatalf("expected 200 from list tickets, got %d: %s", listResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var listPayload struct {
		Tickets []e2eTicketSummary `json:"tickets"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listPayload); err != nil {
		t.Fatalf("decode list tickets response: %v", err)
	}
	if !ticketPresent(listPayload.Tickets, ticketName, objectPath) {
		t.Fatalf("expected ticket %q for path %q in high-level list", ticketName, objectPath)
	}

	getReq := newE2ERequest(t, http.MethodGet, baseURL+"/api/v1/ticket/"+url.PathEscape(ticketName), nil)
	setBasicAuth(getReq)

	getResp, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("perform get ticket request: %v", err)
	}
	defer getResp.Body.Close()

	if getResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getResp.Body)
		t.Fatalf("expected 200 from get ticket, got %d: %s", getResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var getPayload struct {
		Ticket struct {
			Name string `json:"name"`
			Path string `json:"irods_path"`
		} `json:"ticket"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&getPayload); err != nil {
		t.Fatalf("decode get ticket response: %v", err)
	}
	if getPayload.Ticket.Name != ticketName || getPayload.Ticket.Path != objectPath {
		t.Fatalf("unexpected get ticket payload: %+v", getPayload.Ticket)
	}

	deleteReq := newE2ERequest(t, http.MethodDelete, baseURL+"/api/v1/ticket/"+url.PathEscape(ticketName), nil)
	setBasicAuth(deleteReq)

	deleteResp, err := client.Do(deleteReq)
	if err != nil {
		t.Fatalf("perform delete ticket request: %v", err)
	}
	defer deleteResp.Body.Close()

	if deleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleteResp.Body)
		t.Fatalf("expected 204 from delete ticket, got %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
	}

	ticketName = ""

	verifyReq := newE2ERequest(t, http.MethodGet, baseURL+"/api/v1/ticket", nil)
	setBasicAuth(verifyReq)

	verifyResp, err := client.Do(verifyReq)
	if err != nil {
		t.Fatalf("perform verify list tickets request: %v", err)
	}
	defer verifyResp.Body.Close()

	if verifyResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(verifyResp.Body)
		t.Fatalf("expected 200 from verify list tickets, got %d: %s", verifyResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var verifyPayload struct {
		Tickets []e2eTicketSummary `json:"tickets"`
	}
	if err := json.NewDecoder(verifyResp.Body).Decode(&verifyPayload); err != nil {
		t.Fatalf("decode verify list tickets response: %v", err)
	}
	if ticketPresent(verifyPayload.Tickets, createPayload.Ticket.Name, objectPath) {
		t.Fatalf("expected deleted ticket %q to be absent from high-level list", createPayload.Ticket.Name)
	}
}

func TestGetTicketThenDeleteThenGetTicketE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	objectPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-rest-ticket-get-delete-"+randomToken(nil, 8)+".txt",
	)
	content := "irods-go-rest ticket get-delete payload\n"

	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBufferString(content), objectPath, "", false, true, nil); err != nil {
		t.Fatalf("upload ticket get-delete object %q: %v", objectPath, err)
	}
	defer func() {
		if err := filesystem.RemoveFile(objectPath, true); err != nil && filesystem.Exists(objectPath) {
			t.Errorf("cleanup ticket get-delete object %q: %v", objectPath, err)
		}
	}()

	createReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path/ticket", objectPath), strings.NewReader(`{"maximum_uses":5,"lifetime_minutes":30}`))
	createReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createReq)

	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("perform create ticket request: %v", err)
	}
	defer createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201, got %d: %s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var createPayload struct {
		Ticket struct {
			Name string `json:"name"`
			Path string `json:"irods_path"`
		} `json:"ticket"`
	}
	if err := json.NewDecoder(createResp.Body).Decode(&createPayload); err != nil {
		t.Fatalf("decode create ticket response: %v", err)
	}

	ticketName := strings.TrimSpace(createPayload.Ticket.Name)
	if ticketName == "" {
		t.Fatal("expected created ticket name")
	}

	defer func() {
		if ticketName != "" {
			if err := filesystem.DeleteTicket(ticketName); err != nil {
				t.Logf("best-effort cleanup ticket %q: %v", ticketName, err)
			}
		}
	}()

	getBeforeDeleteReq := newE2ERequest(t, http.MethodGet, baseURL+"/api/v1/ticket/"+url.PathEscape(ticketName), nil)
	setBasicAuth(getBeforeDeleteReq)

	getBeforeDeleteResp, err := client.Do(getBeforeDeleteReq)
	if err != nil {
		t.Fatalf("perform get-before-delete ticket request: %v", err)
	}
	defer getBeforeDeleteResp.Body.Close()

	if getBeforeDeleteResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getBeforeDeleteResp.Body)
		t.Fatalf("expected 200 from get-before-delete ticket, got %d: %s", getBeforeDeleteResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var getBeforeDeletePayload struct {
		Ticket struct {
			Name string `json:"name"`
			Path string `json:"irods_path"`
		} `json:"ticket"`
	}
	if err := json.NewDecoder(getBeforeDeleteResp.Body).Decode(&getBeforeDeletePayload); err != nil {
		t.Fatalf("decode get-before-delete ticket response: %v", err)
	}
	if getBeforeDeletePayload.Ticket.Name != ticketName || getBeforeDeletePayload.Ticket.Path != objectPath {
		t.Fatalf("unexpected get-before-delete ticket payload: %+v", getBeforeDeletePayload.Ticket)
	}

	deleteReq := newE2ERequest(t, http.MethodDelete, baseURL+"/api/v1/ticket/"+url.PathEscape(ticketName), nil)
	setBasicAuth(deleteReq)

	deleteResp, err := client.Do(deleteReq)
	if err != nil {
		t.Fatalf("perform delete ticket request: %v", err)
	}
	defer deleteResp.Body.Close()

	if deleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleteResp.Body)
		t.Fatalf("expected 204 from delete ticket, got %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
	}

	ticketName = ""

	getAfterDeleteReq := newE2ERequest(t, http.MethodGet, baseURL+"/api/v1/ticket/"+url.PathEscape(createPayload.Ticket.Name), nil)
	setBasicAuth(getAfterDeleteReq)

	getAfterDeleteResp, err := client.Do(getAfterDeleteReq)
	if err != nil {
		t.Fatalf("perform get-after-delete ticket request: %v", err)
	}
	defer getAfterDeleteResp.Body.Close()

	if getAfterDeleteResp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(getAfterDeleteResp.Body)
		t.Fatalf("expected 404 from get-after-delete ticket, got %d: %s", getAfterDeleteResp.StatusCode, strings.TrimSpace(string(body)))
	}
}

func ticketPresent(tickets []e2eTicketSummary, ticketName string, objectPath string) bool {
	for _, ticket := range tickets {
		if strings.TrimSpace(ticket.Name) == strings.TrimSpace(ticketName) && strings.TrimSpace(ticket.Path) == strings.TrimSpace(objectPath) {
			return true
		}
	}
	return false
}

func pathURLWithTicketID(baseURL string, path string, irodsPath string, ticketID string) string {
	query := url.Values{}
	query.Set("irods_path", irodsPath)
	query.Set("ticket_id", ticketID)
	return strings.TrimRight(baseURL, "/") + path + "?" + query.Encode()
}

func TestHeadPathContentsBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodHead, pathURL(baseURL, "/api/v1/path/contents", fixture.objectPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got == "" {
		t.Fatal("expected Content-Length header")
	}
	if got := resp.Header.Get("Content-Disposition"); !strings.Contains(got, `attachment;`) {
		t.Fatalf("expected Content-Disposition attachment header, got %q", got)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("expected empty HEAD body, got %d bytes", len(body))
	}
}

func TestGetPathContentsInvalidRangeBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/contents", fixture.objectPath), nil)
	req.Header.Set("Range", "bytes=999999-1000000")
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("expected 416, got %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); !strings.HasPrefix(got, "bytes */") {
		t.Fatalf("expected unsatisfied Content-Range header, got %q", got)
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

func waitForIRODSPathFresh(t *testing.T, irodsPath string, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		filesystem := newE2EIRODSFilesystem(t)
		exists := filesystem.Exists(irodsPath)
		filesystem.Release()
		if exists {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func pathURLWithQuery(baseURL string, route string, irodsPath string, extraQuery string) string {
	url := pathURL(baseURL, route, irodsPath)
	if strings.TrimSpace(extraQuery) == "" {
		return url
	}

	return url + "&" + extraQuery
}

type pathAVUE2E struct {
	ID     string `json:"id"`
	Attrib string `json:"attrib"`
	Value  string `json:"value"`
	Unit   string `json:"unit"`
}

func createPathAVUE2E(t *testing.T, client *http.Client, baseURL string, irodsPath string, body string) pathAVUE2E {
	t.Helper()

	req := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path/avu", irodsPath), strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform AVU create request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 from AVU create, got %d", resp.StatusCode)
	}

	var created struct {
		AVU pathAVUE2E `json:"avu"`
	}
	decodeJSON(t, resp.Body, &created)

	if strings.TrimSpace(created.AVU.ID) == "" {
		t.Fatal("expected created AVU id to be populated")
	}

	return created.AVU
}

func decodeJSON(t *testing.T, body io.Reader, target any) {
	t.Helper()

	if err := json.NewDecoder(body).Decode(target); err != nil {
		t.Fatalf("decode json: %v", err)
	}
}
