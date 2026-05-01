//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
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

func TestGetPathChildrenWildcardSearchBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	parentPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-path-search-"+randomToken(nil, 8),
	)
	nestedPath := irodsJoin(parentPath, "nested")

	if err := filesystem.MakeDir(parentPath, true); err != nil {
		t.Fatalf("make parent collection %q: %v", parentPath, err)
	}
	if err := filesystem.MakeDir(nestedPath, true); err != nil {
		t.Fatalf("make nested collection %q: %v", nestedPath, err)
	}
	defer func() {
		if err := filesystem.RemoveDir(parentPath, true, true); err != nil && filesystem.Exists(parentPath) {
			t.Errorf("cleanup parent collection %q: %v", parentPath, err)
		}
	}()

	createChild := func(parent string, childName string, kind string) {
		t.Helper()

		createReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path", parent), strings.NewReader(`{"child_name":"`+childName+`","kind":"`+kind+`"}`))
		createReq.Header.Set("Content-Type", "application/json")
		setBasicAuth(createReq)

		createResp, err := client.Do(createReq)
		if err != nil {
			t.Fatalf("perform create child request for %q under %q: %v", childName, parent, err)
		}
		defer createResp.Body.Close()

		if createResp.StatusCode != http.StatusCreated {
			body, _ := io.ReadAll(createResp.Body)
			t.Fatalf("expected 201 creating %q under %q, got %d: %s", childName, parent, createResp.StatusCode, strings.TrimSpace(string(body)))
		}
	}

	createChild(parentPath, "findme-root-1.txt", "data_object")
	createChild(parentPath, "ignore-root-1.txt", "data_object")
	createChild(nestedPath, "findme-nested-1.txt", "data_object")

	rootMatchPath := irodsJoin(parentPath, "findme-root-1.txt")
	nestedMatchPath := irodsJoin(nestedPath, "findme-nested-1.txt")
	if !waitForIRODSPathFresh(t, rootMatchPath, 3*time.Second) {
		t.Fatalf("expected root test file %q to exist", rootMatchPath)
	}
	if !waitForIRODSPathFresh(t, nestedMatchPath, 3*time.Second) {
		t.Fatalf("expected nested test file %q to exist", nestedMatchPath)
	}

	requestSearch := func(extraQuery string) (int, []string, int) {
		t.Helper()

		req := newE2ERequest(t, http.MethodGet, pathURLWithQuery(baseURL, "/api/v1/path/children", parentPath, extraQuery), nil)
		setBasicAuth(req)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("perform search request %q: %v", extraQuery, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 200 for search request %q, got %d: %s", extraQuery, resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var payload struct {
			Children []struct {
				Path string `json:"path"`
			} `json:"children"`
			Search struct {
				MatchedCount int `json:"matched_count"`
			} `json:"search"`
		}
		decodeJSON(t, resp.Body, &payload)

		paths := make([]string, 0, len(payload.Children))
		for _, child := range payload.Children {
			paths = append(paths, child.Path)
		}

		return len(payload.Children), paths, payload.Search.MatchedCount
	}

	count, paths, matched := requestSearch("name_pattern=findme*&search_scope=children")
	if count != 1 || matched != 1 {
		t.Fatalf("expected one non-recursive match, got count=%d matched_count=%d paths=%+v", count, matched, paths)
	}
	if paths[0] != rootMatchPath {
		t.Fatalf("expected non-recursive result %q, got %+v", rootMatchPath, paths)
	}

	count, paths, matched = requestSearch("name_pattern=findme*&search_scope=subtree")
	if count != 2 || matched != 2 {
		t.Fatalf("expected two recursive matches, got count=%d matched_count=%d paths=%+v", count, matched, paths)
	}

	foundRoot := false
	foundNested := false
	for _, candidate := range paths {
		if candidate == rootMatchPath {
			foundRoot = true
		}
		if candidate == nestedMatchPath {
			foundNested = true
		}
	}
	if !foundRoot || !foundNested {
		t.Fatalf("expected recursive matches %q and %q, got %+v", rootMatchPath, nestedMatchPath, paths)
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

func TestPathMoveCopyFileAndCollectionBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	rootPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-path-relocate-"+randomToken(nil, 8),
	)
	if err := filesystem.MakeDir(rootPath, true); err != nil {
		t.Fatalf("make root collection %q: %v", rootPath, err)
	}
	defer func() {
		if err := filesystem.RemoveDir(rootPath, true, true); err != nil && filesystem.Exists(rootPath) {
			t.Errorf("cleanup root collection %q: %v", rootPath, err)
		}
	}()

	sourceFileParent := irodsJoin(rootPath, "source-files")
	targetFileParent := irodsJoin(rootPath, "target-files")
	if err := filesystem.MakeDir(sourceFileParent, true); err != nil {
		t.Fatalf("make source file parent %q: %v", sourceFileParent, err)
	}
	if err := filesystem.MakeDir(targetFileParent, true); err != nil {
		t.Fatalf("make target file parent %q: %v", targetFileParent, err)
	}

	createFileReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path", sourceFileParent), strings.NewReader(`{"child_name":"source-file.txt","kind":"data_object"}`))
	createFileReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createFileReq)

	createFileResp, err := client.Do(createFileReq)
	if err != nil {
		t.Fatalf("perform create source file request: %v", err)
	}
	defer createFileResp.Body.Close()
	if createFileResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createFileResp.Body)
		t.Fatalf("expected 201 creating source file, got %d: %s", createFileResp.StatusCode, strings.TrimSpace(string(body)))
	}

	sourceFilePath := irodsJoin(sourceFileParent, "source-file.txt")
	movedFilePath := irodsJoin(targetFileParent, "moved-file.txt")
	copiedFilePath := irodsJoin(targetFileParent, "copied-file.txt")
	if !waitForIRODSPathFresh(t, sourceFilePath, 3*time.Second) {
		t.Fatalf("expected source file %q to exist", sourceFilePath)
	}

	moveFileReq := newE2ERequest(t, http.MethodPatch, pathURL(baseURL, "/api/v1/path", sourceFilePath), strings.NewReader(`{"operation":"move","destination_path":"`+movedFilePath+`"}`))
	moveFileReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(moveFileReq)

	moveFileResp, err := client.Do(moveFileReq)
	if err != nil {
		t.Fatalf("perform move file request: %v", err)
	}
	defer moveFileResp.Body.Close()
	if moveFileResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(moveFileResp.Body)
		t.Fatalf("expected 200 moving file, got %d: %s", moveFileResp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !waitForIRODSPathFresh(t, movedFilePath, 3*time.Second) {
		t.Fatalf("expected moved file %q to exist", movedFilePath)
	}
	if waitForIRODSPathFresh(t, sourceFilePath, 500*time.Millisecond) {
		t.Fatalf("expected source file %q to be absent after move", sourceFilePath)
	}

	copyFileReq := newE2ERequest(t, http.MethodPatch, pathURL(baseURL, "/api/v1/path", movedFilePath), strings.NewReader(`{"operation":"copy","destination_path":"`+copiedFilePath+`"}`))
	copyFileReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(copyFileReq)

	copyFileResp, err := client.Do(copyFileReq)
	if err != nil {
		t.Fatalf("perform copy file request: %v", err)
	}
	defer copyFileResp.Body.Close()
	if copyFileResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(copyFileResp.Body)
		t.Fatalf("expected 200 copying file, got %d: %s", copyFileResp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !waitForIRODSPathFresh(t, copiedFilePath, 3*time.Second) {
		t.Fatalf("expected copied file %q to exist", copiedFilePath)
	}
	if !waitForIRODSPathFresh(t, movedFilePath, 500*time.Millisecond) {
		t.Fatalf("expected moved source file %q to remain after copy", movedFilePath)
	}

	sourceCollection := irodsJoin(rootPath, "source-collection")
	sourceCollectionChild := irodsJoin(sourceCollection, "inside.txt")
	if err := filesystem.MakeDir(sourceCollection, true); err != nil {
		t.Fatalf("make source collection %q: %v", sourceCollection, err)
	}
	createCollectionFileReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path", sourceCollection), strings.NewReader(`{"child_name":"inside.txt","kind":"data_object"}`))
	createCollectionFileReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createCollectionFileReq)

	createCollectionFileResp, err := client.Do(createCollectionFileReq)
	if err != nil {
		t.Fatalf("perform create collection child file request: %v", err)
	}
	defer createCollectionFileResp.Body.Close()
	if createCollectionFileResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createCollectionFileResp.Body)
		t.Fatalf("expected 201 creating collection child file, got %d: %s", createCollectionFileResp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !waitForIRODSPathFresh(t, sourceCollectionChild, 3*time.Second) {
		t.Fatalf("expected source collection child %q to exist", sourceCollectionChild)
	}

	movedCollection := irodsJoin(rootPath, "moved-collection")
	movedCollectionChild := irodsJoin(movedCollection, "inside.txt")
	copiedCollection := irodsJoin(rootPath, "copied-collection")
	copiedCollectionChild := irodsJoin(copiedCollection, "inside.txt")

	moveCollectionReq := newE2ERequest(t, http.MethodPatch, pathURL(baseURL, "/api/v1/path", sourceCollection), strings.NewReader(`{"operation":"move","destination_path":"`+movedCollection+`"}`))
	moveCollectionReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(moveCollectionReq)

	moveCollectionResp, err := client.Do(moveCollectionReq)
	if err != nil {
		t.Fatalf("perform move collection request: %v", err)
	}
	defer moveCollectionResp.Body.Close()
	if moveCollectionResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(moveCollectionResp.Body)
		t.Fatalf("expected 200 moving collection, got %d: %s", moveCollectionResp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !waitForIRODSPathFresh(t, movedCollection, 3*time.Second) {
		t.Fatalf("expected moved collection %q to exist", movedCollection)
	}
	if !waitForIRODSPathFresh(t, movedCollectionChild, 3*time.Second) {
		t.Fatalf("expected moved collection child %q to exist", movedCollectionChild)
	}
	if waitForIRODSPathFresh(t, sourceCollection, 500*time.Millisecond) {
		t.Fatalf("expected source collection %q to be absent after move", sourceCollection)
	}

	copyCollectionReq := newE2ERequest(t, http.MethodPatch, pathURL(baseURL, "/api/v1/path", movedCollection), strings.NewReader(`{"operation":"copy","destination_path":"`+copiedCollection+`"}`))
	copyCollectionReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(copyCollectionReq)

	copyCollectionResp, err := client.Do(copyCollectionReq)
	if err != nil {
		t.Fatalf("perform copy collection request: %v", err)
	}
	defer copyCollectionResp.Body.Close()
	if copyCollectionResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(copyCollectionResp.Body)
		t.Fatalf("expected 200 copying collection, got %d: %s", copyCollectionResp.StatusCode, strings.TrimSpace(string(body)))
	}
	if !waitForIRODSPathFresh(t, copiedCollection, 3*time.Second) {
		t.Fatalf("expected copied collection %q to exist", copiedCollection)
	}
	if !waitForIRODSPathFresh(t, copiedCollectionChild, 3*time.Second) {
		t.Fatalf("expected copied collection child %q to exist", copiedCollectionChild)
	}
	if !waitForIRODSPathFresh(t, movedCollection, 500*time.Millisecond) {
		t.Fatalf("expected moved collection %q to remain after copy", movedCollection)
	}
}

func TestPathReplicaResourceLifecycleBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	testResource1 := e2eTestResource1(t)
	testResource2 := e2eTestResource2(t)
	if testResource1 == testResource2 {
		t.Fatalf("TestResource1 and TestResource2 must differ, got %q", testResource1)
	}

	parentPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-path-replica-"+randomToken(nil, 8),
	)
	if err := filesystem.MakeDir(parentPath, true); err != nil {
		t.Fatalf("make parent collection %q: %v", parentPath, err)
	}
	defer func() {
		if err := filesystem.RemoveDir(parentPath, true, true); err != nil && filesystem.Exists(parentPath) {
			t.Errorf("cleanup parent collection %q: %v", parentPath, err)
		}
	}()

	createReq := newE2ERequest(t, http.MethodPost, pathURL(baseURL, "/api/v1/path", parentPath), strings.NewReader(`{"child_name":"replica-source.txt","kind":"data_object"}`))
	createReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createReq)

	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("perform create replica source request: %v", err)
	}
	defer createResp.Body.Close()
	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201 creating replica source, got %d: %s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var created struct {
		Path string `json:"path"`
	}
	decodeJSON(t, createResp.Body, &created)

	sourcePath := parentPath + "/replica-source.txt"
	if created.Path != sourcePath {
		t.Fatalf("expected created path %q, got %q", sourcePath, created.Path)
	}
	if !waitForIRODSPathFresh(t, sourcePath, 3*time.Second) {
		t.Fatalf("expected created data object %q to exist", sourcePath)
	}

	type replicaSummary struct {
		ResourceName string `json:"resource_name"`
	}
	type replicaPayload struct {
		Replicas []replicaSummary `json:"replicas"`
	}

	readReplicaResources := func(replicas []replicaSummary) map[string]struct{} {
		set := make(map[string]struct{}, len(replicas))
		for _, replica := range replicas {
			resourceName := strings.TrimSpace(replica.ResourceName)
			if resourceName != "" {
				set[resourceName] = struct{}{}
			}
		}
		return set
	}

	requestReplicas := func(method string, payload any, expectedStatus int) replicaPayload {
		t.Helper()

		var bodyReader io.Reader
		if payload != nil {
			rawBody, marshalErr := json.Marshal(payload)
			if marshalErr != nil {
				t.Fatalf("marshal %s replica payload: %v", method, marshalErr)
			}
			bodyReader = bytes.NewReader(rawBody)
		}

		req := newE2ERequest(t, method, pathURL(baseURL, "/api/v1/path/replicas", sourcePath), bodyReader)
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		setBasicAuth(req)

		resp, reqErr := client.Do(req)
		if reqErr != nil {
			t.Fatalf("perform %s path replicas request: %v", method, reqErr)
		}
		defer resp.Body.Close()

		if resp.StatusCode != expectedStatus {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected %d for %s /api/v1/path/replicas, got %d: %s", expectedStatus, method, resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var parsed replicaPayload
		decodeJSON(t, resp.Body, &parsed)
		return parsed
	}

	requirePathResource := func(expectedResource string) {
		t.Helper()

		req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path", sourcePath), nil)
		setBasicAuth(req)

		resp, reqErr := client.Do(req)
		if reqErr != nil {
			t.Fatalf("perform get path request for resource verification: %v", reqErr)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 200 for /api/v1/path resource verification, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var payload struct {
			Resource string `json:"resource"`
		}
		decodeJSON(t, resp.Body, &payload)

		if strings.TrimSpace(payload.Resource) != expectedResource {
			t.Fatalf("expected final resource %q, got %q", expectedResource, strings.TrimSpace(payload.Resource))
		}
	}

	// Ensure the data object has a replica on testResource1 before proceeding.
	initialReplicaPayload := requestReplicas(http.MethodGet, nil, http.StatusOK)
	initialResources := readReplicaResources(initialReplicaPayload.Replicas)
	if _, exists := initialResources[testResource1]; !exists {
		var sourceResource string
		for resourceName := range initialResources {
			sourceResource = resourceName
			break
		}
		if sourceResource == "" {
			t.Fatal("expected at least one initial replica resource")
		}

		movedToTestResource1 := requestReplicas(http.MethodPatch, map[string]any{
			"source_resource":      sourceResource,
			"destination_resource": testResource1,
			"update":               true,
			"min_copies":           1,
		}, http.StatusOK)

		resourcesAfterMoveToResource1 := readReplicaResources(movedToTestResource1.Replicas)
		if _, existsAfterMove := resourcesAfterMoveToResource1[testResource1]; !existsAfterMove {
			t.Fatalf("expected replica on %q after initial move, got %v", testResource1, resourcesAfterMoveToResource1)
		}
	}

	// Replicate the data object to testResource2.
	replicatedPayload := requestReplicas(http.MethodPost, map[string]any{
		"resource": testResource2,
		"update":   true,
	}, http.StatusCreated)

	resourcesAfterReplication := readReplicaResources(replicatedPayload.Replicas)
	if _, exists := resourcesAfterReplication[testResource1]; !exists {
		t.Fatalf("expected replica on %q after replication, got %v", testResource1, resourcesAfterReplication)
	}
	if _, exists := resourcesAfterReplication[testResource2]; !exists {
		t.Fatalf("expected replica on %q after replication, got %v", testResource2, resourcesAfterReplication)
	}

	// Delete the replica in testResource1.
	trimmedPayload := requestReplicas(http.MethodDelete, map[string]any{
		"resource":   testResource1,
		"min_copies": 1,
	}, http.StatusOK)

	resourcesAfterTrim := readReplicaResources(trimmedPayload.Replicas)
	if _, exists := resourcesAfterTrim[testResource1]; exists {
		t.Fatalf("expected replica on %q to be removed, got %v", testResource1, resourcesAfterTrim)
	}
	if _, exists := resourcesAfterTrim[testResource2]; !exists {
		t.Fatalf("expected replica on %q to remain after trim, got %v", testResource2, resourcesAfterTrim)
	}

	// Phymove from testResource2 back to testResource1.
	movedBackPayload := requestReplicas(http.MethodPatch, map[string]any{
		"source_resource":      testResource2,
		"destination_resource": testResource1,
		"update":               true,
		"min_copies":           1,
	}, http.StatusOK)

	finalResources := readReplicaResources(movedBackPayload.Replicas)
	if _, exists := finalResources[testResource1]; !exists {
		t.Fatalf("expected replica on %q after move back, got %v", testResource1, finalResources)
	}
	if _, exists := finalResources[testResource2]; exists {
		t.Fatalf("expected replica on %q to be trimmed by move, got %v", testResource2, finalResources)
	}

	requirePathResource(testResource1)
}

func TestPathContentsUploadBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	parentPath := irodsJoin(
		"/"+e2eIRODSZone(t)+"/home/"+e2eBasicUsername(t),
		"e2e-path-upload-"+randomToken(nil, 8),
	)
	if err := filesystem.MakeDir(parentPath, true); err != nil {
		t.Fatalf("make parent collection %q: %v", parentPath, err)
	}
	defer func() {
		if err := filesystem.RemoveDir(parentPath, true, true); err != nil && filesystem.Exists(parentPath) {
			t.Errorf("cleanup parent collection %q: %v", parentPath, err)
		}
	}()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("parent_path", parentPath); err != nil {
		t.Fatalf("write parent_path: %v", err)
	}
	if err := writer.WriteField("file_name", "uploaded.txt"); err != nil {
		t.Fatalf("write file_name: %v", err)
	}
	if err := writer.WriteField("checksum", "true"); err != nil {
		t.Fatalf("write checksum: %v", err)
	}
	part, err := writer.CreateFormFile("content", "uploaded.txt")
	if err != nil {
		t.Fatalf("create content part: %v", err)
	}
	if _, err := part.Write([]byte("uploaded e2e payload\n")); err != nil {
		t.Fatalf("write content part: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

	req := newE2ERequest(t, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/path/contents", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform upload request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var uploaded struct {
		Path       string `json:"path"`
		ParentPath string `json:"parent_path"`
		FileName   string `json:"file_name"`
		Action     string `json:"action"`
		Size       int64  `json:"size"`
		Checksum   struct {
			Requested bool   `json:"requested"`
			Verified  bool   `json:"verified"`
			Algorithm string `json:"algorithm"`
			Value     string `json:"value"`
		} `json:"checksum"`
	}
	decodeJSON(t, resp.Body, &uploaded)

	uploadedPath := parentPath + "/uploaded.txt"
	if uploaded.Path != uploadedPath || uploaded.ParentPath != parentPath || uploaded.FileName != "uploaded.txt" {
		t.Fatalf("unexpected upload payload %+v", uploaded)
	}
	if uploaded.Action != "created" {
		t.Fatalf("expected action created, got %q", uploaded.Action)
	}
	if uploaded.Size != int64(len("uploaded e2e payload\n")) {
		t.Fatalf("expected upload size %d, got %d", len("uploaded e2e payload\n"), uploaded.Size)
	}
	if !uploaded.Checksum.Requested || !uploaded.Checksum.Verified {
		t.Fatalf("expected checksum verification, got %+v", uploaded.Checksum)
	}
	if !waitForIRODSPathFresh(t, uploadedPath, 3*time.Second) {
		t.Fatalf("expected uploaded file %q to exist", uploadedPath)
	}

	downloadReq := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/contents", uploadedPath), nil)
	setBasicAuth(downloadReq)

	downloadResp, err := client.Do(downloadReq)
	if err != nil {
		t.Fatalf("perform upload verification download request: %v", err)
	}
	defer downloadResp.Body.Close()

	if downloadResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(downloadResp.Body)
		t.Fatalf("expected 200, got %d: %s", downloadResp.StatusCode, strings.TrimSpace(string(body)))
	}

	downloaded, err := io.ReadAll(downloadResp.Body)
	if err != nil {
		t.Fatalf("read downloaded payload: %v", err)
	}
	if string(downloaded) != "uploaded e2e payload\n" {
		t.Fatalf("expected uploaded payload %q, got %q", "uploaded e2e payload\n", string(downloaded))
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

func TestGetPathACLsBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	tests := []struct {
		name      string
		irodsPath string
		kind      string
	}{
		{
			name:      "collection",
			irodsPath: fixture.collectionPath,
			kind:      "collection",
		},
		{
			name:      "data object",
			irodsPath: fixture.objectPath,
			kind:      "data_object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/acl", tt.irodsPath), nil)
			setBasicAuth(req)

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("perform request: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected 200, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}

			var payload pathACLE2E
			decodeJSON(t, resp.Body, &payload)
			assertPathACLE2E(t, payload, tt.irodsPath, tt.kind)
		})
	}
}

func TestPathACLReflectsCreatedUserPermissionE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()
	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	zone := e2eIRODSZone(t)
	createdUsername := "e2e-acl-user-" + randomToken(nil, 8)
	adminUsername := e2eIRODSUser(t)
	adminPassword := e2eIRODSPassword(t)

	createReq := newE2ERequest(
		t,
		http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/api/v1/user?zone="+url.QueryEscape(zone),
		strings.NewReader(`{"name":"`+createdUsername+`","type":"rodsuser"}`),
	)
	createReq.Header.Set("Content-Type", "application/json")
	setBasicAuthCredentials(createReq, adminUsername, adminPassword)

	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("perform create user request: %v", err)
	}
	defer createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201, got %d: %s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var createdPayload struct {
		User struct {
			Name string `json:"name"`
			Zone string `json:"zone"`
			Type string `json:"type"`
		} `json:"user"`
	}
	decodeJSON(t, createResp.Body, &createdPayload)

	if createdPayload.User.Name != createdUsername {
		t.Fatalf("expected created username %q, got %q", createdUsername, createdPayload.User.Name)
	}
	if createdPayload.User.Type != "rodsuser" {
		t.Fatalf("expected created user type %q, got %q", "rodsuser", createdPayload.User.Type)
	}

	defer func() {
		deleteReq := newE2ERequest(
			t,
			http.MethodDelete,
			strings.TrimRight(baseURL, "/")+"/api/v1/user/"+url.PathEscape(createdUsername)+"?zone="+url.QueryEscape(zone),
			nil,
		)
		setBasicAuthCredentials(deleteReq, adminUsername, adminPassword)

		deleteResp, err := client.Do(deleteReq)
		if err != nil {
			t.Errorf("perform delete user request: %v", err)
			return
		}
		defer deleteResp.Body.Close()

		if deleteResp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(deleteResp.Body)
			t.Errorf("expected 204 from delete user, got %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
		}
	}()

	if err := filesystem.ChangeACLs(fixture.collectionPath, irodstypes.IRODSAccessLevelReadObject, createdUsername, zone, false, false); err != nil {
		t.Fatalf("grant ACL to created user: %v", err)
	}

	defer func() {
		if err := filesystem.ChangeACLs(fixture.collectionPath, irodstypes.IRODSAccessLevelNull, createdUsername, zone, false, false); err != nil {
			t.Errorf("remove ACL from created user: %v", err)
		}
	}()

	var aclPayload pathACLE2E
	foundUser := false
	deadline := time.Now().Add(5 * time.Second)
	for !foundUser && time.Now().Before(deadline) {
		req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/acl", fixture.collectionPath), nil)
		setBasicAuth(req)

		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("perform ACL request: %v", err)
		}

		func() {
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected 200, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			}

			decodeJSON(t, resp.Body, &aclPayload)
		}()

		for _, user := range aclPayload.Users {
			if user.Name == createdUsername && user.Zone == zone {
				foundUser = true
				if user.AccessLevel != string(irodstypes.IRODSAccessLevelReadObject) {
					t.Fatalf("expected user ACL access_level %q, got %q", irodstypes.IRODSAccessLevelReadObject, user.AccessLevel)
				}
				break
			}
		}

		if !foundUser {
			time.Sleep(100 * time.Millisecond)
		}
	}

	if !foundUser {
		t.Fatalf("expected ACL users to include %q in zone %q, got %+v", createdUsername, zone, aclPayload.Users)
	}
}

func TestPostPutDeletePathACLBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	zone := e2eIRODSZone(t)
	createdUsername := "e2e-acl-mutation-user-" + randomToken(nil, 8)
	deleteCreatedUser := createE2EUser(t, client, baseURL, zone, createdUsername)
	defer deleteCreatedUser()

	createReq := newE2ERequest(
		t,
		http.MethodPost,
		pathURL(baseURL, "/api/v1/path/acl", fixture.collectionPath),
		strings.NewReader(`{"name":"`+createdUsername+`","zone":"`+zone+`","type":"user","access_level":"read_object"}`),
	)
	createReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(createReq)

	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("perform ACL create request: %v", err)
	}
	defer createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201, got %d: %s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var createPayload struct {
		IRODSPath string          `json:"irods_path"`
		ACL       pathACLEntryE2E `json:"acl"`
	}
	decodeJSON(t, createResp.Body, &createPayload)

	if createPayload.IRODSPath != fixture.collectionPath {
		t.Fatalf("expected irods_path %q, got %q", fixture.collectionPath, createPayload.IRODSPath)
	}
	if createPayload.ACL.Name != createdUsername || createPayload.ACL.Zone != zone {
		t.Fatalf("unexpected created ACL principal %+v", createPayload.ACL)
	}
	if createPayload.ACL.AccessLevel != string(irodstypes.IRODSAccessLevelReadObject) {
		t.Fatalf("expected created ACL access level %q, got %q", irodstypes.IRODSAccessLevelReadObject, createPayload.ACL.AccessLevel)
	}
	if strings.TrimSpace(createPayload.ACL.ID) == "" {
		t.Fatal("expected created ACL id to be populated")
	}

	if !waitForPathACLUserAccessLevel(t, client, baseURL, fixture.collectionPath, createdUsername, zone, string(irodstypes.IRODSAccessLevelReadObject), 5*time.Second) {
		t.Fatalf("expected ACL user %q to appear with access level %q", createdUsername, irodstypes.IRODSAccessLevelReadObject)
	}

	updateReq := newE2ERequest(
		t,
		http.MethodPut,
		strings.TrimRight(baseURL, "/")+"/api/v1/path/acl/"+url.PathEscape(createPayload.ACL.ID)+"?irods_path="+url.QueryEscape(fixture.collectionPath),
		strings.NewReader(`{"access_level":"modify_object","recursive":false}`),
	)
	updateReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(updateReq)

	updateResp, err := client.Do(updateReq)
	if err != nil {
		t.Fatalf("perform ACL update request: %v", err)
	}
	defer updateResp.Body.Close()

	if updateResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(updateResp.Body)
		t.Fatalf("expected 200, got %d: %s", updateResp.StatusCode, strings.TrimSpace(string(body)))
	}

	var updatePayload struct {
		ACL pathACLEntryE2E `json:"acl"`
	}
	decodeJSON(t, updateResp.Body, &updatePayload)

	if updatePayload.ACL.AccessLevel != string(irodstypes.IRODSAccessLevelModifyObject) {
		t.Fatalf("expected updated ACL access level %q, got %q", irodstypes.IRODSAccessLevelModifyObject, updatePayload.ACL.AccessLevel)
	}

	if !waitForPathACLUserAccessLevel(t, client, baseURL, fixture.collectionPath, createdUsername, zone, string(irodstypes.IRODSAccessLevelModifyObject), 5*time.Second) {
		t.Fatalf("expected ACL user %q to update with access level %q", createdUsername, irodstypes.IRODSAccessLevelModifyObject)
	}

	deleteReq := newE2ERequest(
		t,
		http.MethodDelete,
		strings.TrimRight(baseURL, "/")+"/api/v1/path/acl/"+url.PathEscape(createPayload.ACL.ID)+"?irods_path="+url.QueryEscape(fixture.collectionPath),
		nil,
	)
	setBasicAuth(deleteReq)

	deleteResp, err := client.Do(deleteReq)
	if err != nil {
		t.Fatalf("perform ACL delete request: %v", err)
	}
	defer deleteResp.Body.Close()

	if deleteResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(deleteResp.Body)
		t.Fatalf("expected 204, got %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
	}

	if !waitForPathACLUserAbsent(t, client, baseURL, fixture.collectionPath, createdUsername, zone, 5*time.Second) {
		t.Fatalf("expected ACL user %q to be removed", createdUsername)
	}
}

func TestPutDeletePathACLInheritanceBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()

	initialACL := fetchPathACLE2E(t, client, baseURL, fixture.collectionPath)
	initialInheritance := false
	if initialACL.InheritanceEnabled != nil {
		initialInheritance = *initialACL.InheritanceEnabled
	}
	defer func() {
		restoreReq := newE2ERequest(
			t,
			http.MethodPut,
			strings.TrimRight(baseURL, "/")+"/api/v1/path/acl/inheritance?irods_path="+url.QueryEscape(fixture.collectionPath),
			strings.NewReader(`{"enabled":`+strconv.FormatBool(initialInheritance)+`,"recursive":false}`),
		)
		restoreReq.Header.Set("Content-Type", "application/json")
		setBasicAuth(restoreReq)

		restoreResp, err := client.Do(restoreReq)
		if err != nil {
			t.Errorf("perform ACL inheritance restore request: %v", err)
			return
		}
		defer restoreResp.Body.Close()

		if restoreResp.StatusCode != http.StatusNoContent {
			body, _ := io.ReadAll(restoreResp.Body)
			t.Errorf("expected 204 from inheritance restore, got %d: %s", restoreResp.StatusCode, strings.TrimSpace(string(body)))
		}
	}()

	enableReq := newE2ERequest(
		t,
		http.MethodPut,
		strings.TrimRight(baseURL, "/")+"/api/v1/path/acl/inheritance?irods_path="+url.QueryEscape(fixture.collectionPath),
		strings.NewReader(`{"enabled":true,"recursive":false}`),
	)
	enableReq.Header.Set("Content-Type", "application/json")
	setBasicAuth(enableReq)

	enableResp, err := client.Do(enableReq)
	if err != nil {
		t.Fatalf("perform ACL inheritance enable request: %v", err)
	}
	defer enableResp.Body.Close()

	if enableResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(enableResp.Body)
		t.Fatalf("expected 204, got %d: %s", enableResp.StatusCode, strings.TrimSpace(string(body)))
	}

	if !waitForPathACLInheritanceState(t, client, baseURL, fixture.collectionPath, true, 5*time.Second) {
		t.Fatal("expected ACL inheritance to become enabled")
	}

	disableReq := newE2ERequest(
		t,
		http.MethodDelete,
		strings.TrimRight(baseURL, "/")+"/api/v1/path/acl/inheritance?irods_path="+url.QueryEscape(fixture.collectionPath)+"&recursive=false",
		nil,
	)
	setBasicAuth(disableReq)

	disableResp, err := client.Do(disableReq)
	if err != nil {
		t.Fatalf("perform ACL inheritance disable request: %v", err)
	}
	defer disableResp.Body.Close()

	if disableResp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(disableResp.Body)
		t.Fatalf("expected 204, got %d: %s", disableResp.StatusCode, strings.TrimSpace(string(body)))
	}

	if !waitForPathACLInheritanceState(t, client, baseURL, fixture.collectionPath, false, 5*time.Second) {
		t.Fatal("expected ACL inheritance to become disabled")
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

type actionLinkE2E struct {
	Href   string `json:"href"`
	Method string `json:"method"`
}

type pathACLE2E struct {
	IRODSPath          string `json:"irods_path"`
	Kind               string `json:"kind"`
	InheritanceEnabled *bool  `json:"inheritance_enabled"`
	PathSegments       []struct {
		DisplayName string `json:"display_name"`
		IRODSPath   string `json:"irods_path"`
		Href        string `json:"href"`
	} `json:"path_segments"`
	Links struct {
		Path           actionLinkE2E `json:"path"`
		AddUser        actionLinkE2E `json:"add_user"`
		SetInheritance actionLinkE2E `json:"set_inheritance"`
	} `json:"links"`
	Users  []pathACLEntryE2E `json:"users"`
	Groups []pathACLEntryE2E `json:"groups"`
}

type pathACLEntryE2E struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Zone          string `json:"zone"`
	Type          string `json:"type"`
	IRODSUserType string `json:"irods_user_type"`
	AccessLevel   string `json:"access_level"`
	Links         struct {
		Update actionLinkE2E `json:"update"`
		Remove actionLinkE2E `json:"remove"`
	} `json:"links"`
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

func assertPathACLE2E(t *testing.T, payload pathACLE2E, irodsPath string, kind string) {
	t.Helper()

	if payload.IRODSPath != irodsPath {
		t.Fatalf("expected irods_path %q, got %q", irodsPath, payload.IRODSPath)
	}
	if payload.Kind != kind {
		t.Fatalf("expected kind %q, got %q", kind, payload.Kind)
	}
	if len(payload.PathSegments) == 0 {
		t.Fatal("expected path_segments to be populated")
	}
	if payload.Links.Path.Method != http.MethodGet || !strings.Contains(payload.Links.Path.Href, "/api/v1/path?irods_path=") {
		t.Fatalf("expected path GET link, got %+v", payload.Links.Path)
	}
	if payload.Links.AddUser.Method != http.MethodPost || !strings.Contains(payload.Links.AddUser.Href, "/api/v1/path/acl?irods_path=") {
		t.Fatalf("expected add_user POST link, got %+v", payload.Links.AddUser)
	}
	if kind == "collection" {
		if payload.InheritanceEnabled == nil {
			t.Fatal("expected inheritance_enabled to be populated for collections")
		}
		if payload.Links.SetInheritance.Method != http.MethodPut || !strings.Contains(payload.Links.SetInheritance.Href, "/api/v1/path/acl/inheritance?irods_path=") {
			t.Fatalf("expected set_inheritance PUT link, got %+v", payload.Links.SetInheritance)
		}
	}
	if payload.Users == nil {
		t.Fatal("expected users array to be present")
	}
	if payload.Groups == nil {
		t.Fatal("expected groups array to be present")
	}

	expectedUser := e2eBasicUsername(t)
	for _, user := range payload.Users {
		if user.Name != expectedUser {
			continue
		}
		if user.Type != "user" {
			t.Fatalf("expected ACL principal type user, got %+v", user)
		}
		if strings.TrimSpace(user.ID) == "" || strings.TrimSpace(user.AccessLevel) == "" {
			t.Fatalf("expected ACL id and access_level to be populated, got %+v", user)
		}
		if user.Links.Update.Method != http.MethodPut || strings.TrimSpace(user.Links.Update.Href) == "" {
			t.Fatalf("expected user ACL update PUT link, got %+v", user.Links.Update)
		}
		if user.Links.Remove.Method != http.MethodDelete || strings.TrimSpace(user.Links.Remove.Href) == "" {
			t.Fatalf("expected user ACL remove DELETE link, got %+v", user.Links.Remove)
		}
		return
	}

	t.Fatalf("expected ACL users to include %q, got %+v", expectedUser, payload.Users)
}

func createE2EUser(t *testing.T, client *http.Client, baseURL string, zone string, username string) func() {
	t.Helper()

	adminUsername := e2eIRODSUser(t)
	adminPassword := e2eIRODSPassword(t)

	createReq := newE2ERequest(
		t,
		http.MethodPost,
		strings.TrimRight(baseURL, "/")+"/api/v1/user?zone="+url.QueryEscape(zone),
		strings.NewReader(`{"name":"`+username+`","type":"rodsuser"}`),
	)
	createReq.Header.Set("Content-Type", "application/json")
	setBasicAuthCredentials(createReq, adminUsername, adminPassword)

	createResp, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("perform create user request: %v", err)
	}
	defer createResp.Body.Close()

	if createResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(createResp.Body)
		t.Fatalf("expected 201 from create user, got %d: %s", createResp.StatusCode, strings.TrimSpace(string(body)))
	}

	return func() {
		deleteReq := newE2ERequest(
			t,
			http.MethodDelete,
			strings.TrimRight(baseURL, "/")+"/api/v1/user/"+url.PathEscape(username)+"?zone="+url.QueryEscape(zone),
			nil,
		)
		setBasicAuthCredentials(deleteReq, adminUsername, adminPassword)

		deleteResp, err := client.Do(deleteReq)
		if err != nil {
			t.Errorf("perform delete user request: %v", err)
			return
		}
		defer deleteResp.Body.Close()

		if deleteResp.StatusCode != http.StatusNoContent && deleteResp.StatusCode != http.StatusNotFound {
			body, _ := io.ReadAll(deleteResp.Body)
			t.Errorf("expected 204 or 404 from delete user, got %d: %s", deleteResp.StatusCode, strings.TrimSpace(string(body)))
		}
	}
}

func fetchPathACLE2E(t *testing.T, client *http.Client, baseURL string, irodsPath string) pathACLE2E {
	t.Helper()

	req := newE2ERequest(t, http.MethodGet, pathURL(baseURL, "/api/v1/path/acl", irodsPath), nil)
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform ACL request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from ACL request, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload pathACLE2E
	decodeJSON(t, resp.Body, &payload)
	return payload
}

func waitForPathACLUserAccessLevel(t *testing.T, client *http.Client, baseURL string, irodsPath string, username string, zone string, accessLevel string, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		payload := fetchPathACLE2E(t, client, baseURL, irodsPath)
		for _, user := range payload.Users {
			if user.Name == username && user.Zone == zone && user.AccessLevel == accessLevel {
				return true
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	return false
}

func waitForPathACLUserAbsent(t *testing.T, client *http.Client, baseURL string, irodsPath string, username string, zone string, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		payload := fetchPathACLE2E(t, client, baseURL, irodsPath)
		found := false
		for _, user := range payload.Users {
			if user.Name == username && user.Zone == zone {
				found = true
				break
			}
		}

		if !found {
			return true
		}

		time.Sleep(100 * time.Millisecond)
	}

	return false
}

func waitForPathACLInheritanceState(t *testing.T, client *http.Client, baseURL string, irodsPath string, enabled bool, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		payload := fetchPathACLE2E(t, client, baseURL, irodsPath)
		if payload.InheritanceEnabled != nil && *payload.InheritanceEnabled == enabled {
			return true
		}

		time.Sleep(100 * time.Millisecond)
	}

	return false
}

func decodeJSON(t *testing.T, body io.Reader, target any) {
	t.Helper()

	if err := json.NewDecoder(body).Decode(target); err != nil {
		t.Fatalf("decode json: %v", err)
	}
}
