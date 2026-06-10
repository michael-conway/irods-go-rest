//go:build e2e
// +build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
)

func TestPathQueryAVUE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	filesystem := newE2EIRODSFilesystem(t)
	t.Cleanup(filesystem.Release)
	fixture := createPathQueryE2EFixture(t, filesystem)

	includeMatchedAVUs := true
	bothDescendantsRequest := pathQueryE2ERequest{
		IRODSPath:          fixture.root,
		SearchScope:        "descendants",
		Kinds:              []string{"data_object", "collection"},
		Conditions:         pathQueryE2EAVUConditions(fixture.attrName, "frog-*", "habitat:p%"),
		IncludeMatchedAVUs: &includeMatchedAVUs,
		Limit:              50,
	}
	descendantResult := postPathQueryE2E(t, client, baseURL, bothDescendantsRequest)
	assertPathQueryE2EPathsExactly(t, descendantResult.Paths, []string{
		fixture.alpha,
		fixture.beta,
		fixture.gamma,
		fixture.alphaFile,
		fixture.deepFile,
		fixture.bravoFile,
	})
	assertPathQueryE2EPathAbsent(t, descendantResult.Paths, fixture.rootFile)
	assertPathQueryE2EMatchedAVUPresent(t, descendantResult.MatchedAVUs, fixture.alpha, fixture.attrName, "frog-coll-alpha", "habitat:pond")
	assertPathQueryE2EMatchedAVUPresent(t, descendantResult.MatchedAVUs, fixture.alphaFile, fixture.attrName, "frog-file-alpha", "habitat:pond")
	assertPathQueryE2EMatchedAVUPresent(t, descendantResult.MatchedAVUs, fixture.deepFile, fixture.attrName, "frog-file-deep", "habitat:pond")
	assertPathQueryE2EOmitsReplicas(t, descendantResult.Paths, fixture.deepFile)
	assertPathQueryE2EQueryString(t, descendantResult.Query, "search_scope", "descendants")
	assertPathQueryE2EQueryScope(t, descendantResult.Query, fixture.root, "descendants")

	roundTrippedDescendants := postPathQueryE2E(t, client, baseURL, roundTripPathQueryE2ERequest(t, bothDescendantsRequest))
	assertPathQueryE2EPathsExactly(t, roundTrippedDescendants.Paths, pathQueryE2EPaths(descendantResult.Paths))

	dataObjectsChildrenRequest := pathQueryE2ERequest{
		IRODSPath:   fixture.root,
		SearchScope: "children",
		Kinds:       []string{"data_object"},
		Conditions:  pathQueryE2EAVUConditions(fixture.attrName, "frog-*", "habitat:p*"),
		Limit:       20,
	}
	dataObjectsChildrenResult := postPathQueryE2E(t, client, baseURL, dataObjectsChildrenRequest)
	assertPathQueryE2EPathsExactly(t, dataObjectsChildrenResult.Paths, []string{fixture.rootFile})
	assertPathQueryE2EOmitsReplicas(t, dataObjectsChildrenResult.Paths, fixture.rootFile)

	roundTrippedDataObjects := postPathQueryE2E(t, client, baseURL, roundTripPathQueryE2ERequest(t, dataObjectsChildrenRequest))
	assertPathQueryE2EPathsExactly(t, roundTrippedDataObjects.Paths, []string{fixture.rootFile})

	collectionsChildrenRequest := pathQueryE2ERequest{
		IRODSPath:   fixture.root,
		SearchScope: "children",
		Kinds:       []string{"collection"},
		Conditions: []pathQueryE2ECondition{
			{Field: "avu.attrib", Op: "=", Value: fixture.attrName},
			{Field: "avu.value", Op: "like", Value: "frog-coll-*"},
			{Field: "avu.unit", Op: "like", Value: "habitat:p%"},
		},
		Limit: 20,
	}
	collectionsChildrenResult := postPathQueryE2E(t, client, baseURL, collectionsChildrenRequest)
	assertPathQueryE2EPathsExactly(t, collectionsChildrenResult.Paths, []string{fixture.alpha})

	collectionsUnderAlphaRequest := pathQueryE2ERequest{
		IRODSPath:   fixture.alpha,
		SearchScope: "descendants",
		Kinds:       []string{"collection"},
		Conditions:  pathQueryE2EAVUConditions(fixture.attrName, "frog-coll-*", "habitat:p*"),
		Limit:       20,
	}
	collectionsUnderAlphaResult := postPathQueryE2E(t, client, baseURL, collectionsUnderAlphaRequest)
	assertPathQueryE2EPathsExactly(t, collectionsUnderAlphaResult.Paths, []string{fixture.beta, fixture.gamma})
	assertPathQueryE2EPathAbsent(t, collectionsUnderAlphaResult.Paths, fixture.alpha)
	assertPathQueryE2EPathAbsent(t, collectionsUnderAlphaResult.Paths, fixture.bravo)

	roundTrippedCollections := postPathQueryE2E(t, client, baseURL, roundTripPathQueryE2ERequest(t, collectionsUnderAlphaRequest))
	assertPathQueryE2EPathsExactly(t, roundTrippedCollections.Paths, []string{fixture.beta, fixture.gamma})

	firstPageRequest := pathQueryE2ERequest{
		IRODSPath:   fixture.root,
		SearchScope: "children",
		Kinds:       []string{"data_object", "collection"},
		Conditions:  pathQueryE2EAVUConditions(fixture.attrName, "frog-%", "habitat:p%"),
		Limit:       1,
	}
	firstPage := postPathQueryE2E(t, client, baseURL, firstPageRequest)
	if len(firstPage.Paths) != 1 {
		t.Fatalf("expected first page to return 1 entry, got %d", len(firstPage.Paths))
	}
	if !firstPage.Page.HasMore || strings.TrimSpace(firstPage.Page.NextPageToken) == "" {
		t.Fatalf("expected first page to have a next page token: %+v", firstPage.Page)
	}

	secondPageRequest := firstPageRequest
	secondPageRequest.Limit = 10
	secondPageRequest.PageToken = firstPage.Page.NextPageToken
	secondPage := postPathQueryE2E(t, client, baseURL, secondPageRequest)

	childrenEntries := append([]pathQueryE2EPath(nil), firstPage.Paths...)
	childrenEntries = append(childrenEntries, secondPage.Paths...)
	assertPathQueryE2EPathsExactly(t, childrenEntries, []string{fixture.alpha, fixture.rootFile})
	assertPathQueryE2EPathAbsent(t, childrenEntries, fixture.alphaFile)
}

func TestPathQueryMatchedAVUUpdateLinkE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	client := newE2EHTTPClient()

	filesystem := newE2EIRODSFilesystem(t)
	t.Cleanup(filesystem.Release)
	fixture := createPathQueryE2EFixture(t, filesystem)

	includeMatchedAVUs := true
	query := pathQueryE2ERequest{
		IRODSPath:          fixture.root,
		SearchScope:        "descendants",
		Kinds:              []string{"data_object"},
		Conditions:         pathQueryE2EAVUConditions(fixture.attrName, "frog-file-alpha", "habitat:pond"),
		IncludeMatchedAVUs: &includeMatchedAVUs,
		Limit:              10,
	}
	result := postPathQueryE2E(t, client, baseURL, query)
	assertPathQueryE2EPathsExactly(t, result.Paths, []string{fixture.alphaFile})
	matched := requirePathQueryE2EMatchedAVU(t, result.MatchedAVUs, fixture.alphaFile, fixture.attrName, "frog-file-alpha", "habitat:pond")
	assertPathQueryE2EAVUHATEOAS(t, matched)

	replacement := map[string]string{
		"attrib": fixture.attrName,
		"value":  "frog-file-alpha-updated",
		"unit":   "habitat:pond",
	}
	body, err := json.Marshal(replacement)
	if err != nil {
		t.Fatalf("marshal AVU update request: %v", err)
	}

	req := newE2ERequest(t, http.MethodPut, e2eHrefURL(baseURL, matched.Links.Update.Href), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform matched AVU update link request: %v", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read AVU update response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from matched AVU update link, got %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var updated struct {
		AVU pathAVUE2E `json:"avu"`
	}
	if err := json.Unmarshal(responseBody, &updated); err != nil {
		t.Fatalf("decode AVU update response: %v", err)
	}
	if strings.TrimSpace(updated.AVU.ID) == "" {
		t.Fatal("expected updated AVU id to be populated")
	}
	if updated.AVU.Attrib != fixture.attrName || updated.AVU.Value != "frog-file-alpha-updated" || updated.AVU.Unit != "habitat:pond" {
		t.Fatalf("unexpected updated AVU %+v", updated.AVU)
	}

	oldResult := postPathQueryE2E(t, client, baseURL, query)
	assertPathQueryE2EPathsExactly(t, oldResult.Paths, nil)

	updatedQuery := query
	updatedQuery.Conditions = pathQueryE2EAVUConditions(fixture.attrName, "frog-file-alpha-updated", "habitat:pond")
	updatedResult := postPathQueryE2E(t, client, baseURL, updatedQuery)
	assertPathQueryE2EPathsExactly(t, updatedResult.Paths, []string{fixture.alphaFile})
	assertPathQueryE2EMatchedAVUPresent(t, updatedResult.MatchedAVUs, fixture.alphaFile, fixture.attrName, "frog-file-alpha-updated", "habitat:pond")
}

type pathQueryE2EFixture struct {
	root      string
	alpha     string
	beta      string
	gamma     string
	bravo     string
	rootFile  string
	otherFile string
	alphaFile string
	betaFile  string
	deepFile  string
	bravoFile string
	attrName  string
}

type pathQueryE2ERequest struct {
	IRODSPath          string                  `json:"irods_path,omitempty"`
	SearchScope        string                  `json:"search_scope,omitempty"`
	Kinds              []string                `json:"kinds,omitempty"`
	Conditions         []pathQueryE2ECondition `json:"conditions,omitempty"`
	Limit              int                     `json:"limit,omitempty"`
	PageToken          string                  `json:"page_token,omitempty"`
	IncludeMatchedAVUs *bool                   `json:"include_matched_avus,omitempty"`
}

type pathQueryE2ECondition struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value string `json:"value"`
}

func pathQueryE2EAVUConditions(attrib string, value string, unit string) []pathQueryE2ECondition {
	conditions := []pathQueryE2ECondition{}
	for _, candidate := range []struct {
		field string
		value string
	}{
		{field: "avu.attrib", value: attrib},
		{field: "avu.value", value: value},
		{field: "avu.unit", value: unit},
	} {
		value := strings.TrimSpace(candidate.value)
		if value == "" || value == "*" || value == "%" {
			continue
		}
		op := "="
		if strings.ContainsAny(value, "*%") {
			op = "like"
		}
		conditions = append(conditions, pathQueryE2ECondition{
			Field: candidate.field,
			Op:    op,
			Value: value,
		})
	}
	return conditions
}

type pathQueryE2EResponse struct {
	IRODSPath   string                       `json:"irods_path"`
	Paths       []pathQueryE2EPath           `json:"paths"`
	MatchedAVUs map[string][]pathQueryE2EAVU `json:"matched_avus"`
	Page        pathQueryE2EPage             `json:"page"`
	Query       map[string]json.RawMessage   `json:"query"`
}

type pathQueryE2EPath struct {
	Path     string                `json:"path"`
	Kind     string                `json:"kind"`
	Replicas []pathQueryE2EReplica `json:"replicas,omitempty"`
}

type pathQueryE2EReplica struct {
	Number int64 `json:"number"`
}

type pathQueryE2EAVU struct {
	ID     string               `json:"id"`
	Attrib string               `json:"attrib"`
	Value  string               `json:"value"`
	Unit   string               `json:"unit"`
	Links  pathQueryE2EAVULinks `json:"links"`
}

type pathQueryE2EAVULinks struct {
	Update actionLinkE2E `json:"update"`
	Delete actionLinkE2E `json:"delete"`
}

type pathQueryE2EPage struct {
	Limit         int                `json:"limit"`
	HasMore       bool               `json:"has_more"`
	NextPageToken string             `json:"next_page_token"`
	Returned      pathQueryE2ECounts `json:"returned"`
	Scanned       pathQueryE2ECounts `json:"scanned"`
}

type pathQueryE2ECounts struct {
	Collections int `json:"collections"`
	DataObjects int `json:"data_objects"`
}

func createPathQueryE2EFixture(t *testing.T, filesystem *irodsfs.FileSystem) pathQueryE2EFixture {
	t.Helper()

	root := fmt.Sprintf(
		"/%s/home/%s/e2e-path-query-%s",
		e2eIRODSZone(t),
		e2eBasicUsername(t),
		randomToken(nil, 12),
	)
	fixture := pathQueryE2EFixture{
		root:     root,
		attrName: "e2e.entry-query." + randomToken(nil, 12),
	}
	fixture.alpha = irodsJoin(fixture.root, "alpha")
	fixture.beta = irodsJoin(fixture.alpha, "beta")
	fixture.gamma = irodsJoin(fixture.beta, "gamma")
	fixture.bravo = irodsJoin(fixture.root, "bravo")
	fixture.rootFile = irodsJoin(fixture.root, "root-frog.txt")
	fixture.otherFile = irodsJoin(fixture.root, "root-lizard.txt")
	fixture.alphaFile = irodsJoin(fixture.alpha, "alpha-frog.txt")
	fixture.betaFile = irodsJoin(fixture.beta, "beta-toad.txt")
	fixture.deepFile = irodsJoin(fixture.gamma, "deep-frog.txt")
	fixture.bravoFile = irodsJoin(fixture.bravo, "bravo-frog.txt")

	for _, collectionPath := range []string{fixture.gamma, fixture.bravo} {
		if err := filesystem.MakeDir(collectionPath, true); err != nil {
			t.Fatalf("create fixture collection %q: %v", collectionPath, err)
		}
	}
	t.Cleanup(func() {
		if err := filesystem.RemoveDir(fixture.root, true, true); err != nil && filesystem.Exists(fixture.root) {
			t.Errorf("remove fixture root %q: %v", fixture.root, err)
		}
	})

	for filePath, contents := range map[string]string{
		fixture.rootFile:  "root frog fixture\n",
		fixture.otherFile: "root lizard fixture\n",
		fixture.alphaFile: "alpha frog fixture\n",
		fixture.betaFile:  "beta toad fixture\n",
		fixture.deepFile:  "deep frog fixture\n",
		fixture.bravoFile: "bravo frog fixture\n",
	} {
		createPathQueryE2EFile(t, filesystem, filePath, contents)
	}

	for targetPath, avu := range map[string]struct {
		value string
		unit  string
	}{
		fixture.alpha:     {value: "frog-coll-alpha", unit: "habitat:pond"},
		fixture.beta:      {value: "frog-coll-beta", unit: "habitat:pond"},
		fixture.gamma:     {value: "frog-coll-gamma", unit: "habitat:pond"},
		fixture.bravo:     {value: "bear-coll-bravo", unit: "habitat:forest"},
		fixture.rootFile:  {value: "frog-file-root", unit: "habitat:pond"},
		fixture.otherFile: {value: "lizard-file-root", unit: "habitat:pond"},
		fixture.alphaFile: {value: "frog-file-alpha", unit: "habitat:pond"},
		fixture.betaFile:  {value: "toad-file-beta", unit: "habitat:marsh"},
		fixture.deepFile:  {value: "frog-file-deep", unit: "habitat:pond"},
		fixture.bravoFile: {value: "frog-file-bravo", unit: "habitat:pond"},
	} {
		addPathQueryE2EAVU(t, filesystem, targetPath, fixture.attrName, avu.value, avu.unit)
	}

	noiseAttr := fixture.attrName + ".noise"
	addPathQueryE2EAVU(t, filesystem, fixture.otherFile, noiseAttr, "frog-file-noise", "habitat:pond")
	addPathQueryE2EAVU(t, filesystem, fixture.bravo, noiseAttr, "frog-coll-noise", "habitat:pond")

	return fixture
}

func createPathQueryE2EFile(t *testing.T, filesystem *irodsfs.FileSystem, irodsPath string, contents string) {
	t.Helper()

	if _, err := filesystem.UploadFileFromBuffer(bytes.NewBufferString(contents), irodsPath, "", false, false, nil); err != nil {
		t.Fatalf("create fixture file %q: %v", irodsPath, err)
	}
}

func addPathQueryE2EAVU(t *testing.T, filesystem *irodsfs.FileSystem, irodsPath string, attrib string, value string, unit string) {
	t.Helper()

	if err := filesystem.AddMetadata(irodsPath, attrib, value, unit); err != nil {
		t.Fatalf("add metadata to %q: %v", irodsPath, err)
	}
}

func postPathQueryE2E(t *testing.T, client *http.Client, baseURL string, request pathQueryE2ERequest) pathQueryE2EResponse {
	t.Helper()

	return postPathQueryPayloadE2E(t, client, baseURL, request)
}

func postPathQueryPayloadE2E(t *testing.T, client *http.Client, baseURL string, payload any) pathQueryE2EResponse {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal path query request: %v", err)
	}

	req := newE2ERequest(t, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/v1/path/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	setBasicAuth(req)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("perform path query request: %v", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read path query response: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from path query, got %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var response pathQueryE2EResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		t.Fatalf("decode path query response: %v", err)
	}
	return response
}

func roundTripPathQueryE2ERequest(t *testing.T, request pathQueryE2ERequest) pathQueryE2ERequest {
	t.Helper()

	data, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal path query request for round trip: %v", err)
	}

	var roundTripped pathQueryE2ERequest
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("unmarshal path query request for round trip: %v", err)
	}
	return roundTripped
}

func assertPathQueryE2EPathsExactly(t *testing.T, entries []pathQueryE2EPath, expectedPaths []string) {
	t.Helper()

	actualPaths := pathQueryE2EPaths(entries)
	sort.Strings(actualPaths)
	expected := append([]string(nil), expectedPaths...)
	sort.Strings(expected)

	if len(actualPaths) != len(expected) {
		t.Fatalf("expected paths %+v, got %+v", expected, actualPaths)
	}
	for idx := range expected {
		if actualPaths[idx] != expected[idx] {
			t.Fatalf("expected paths %+v, got %+v", expected, actualPaths)
		}
	}
}

func assertPathQueryE2EPathAbsent(t *testing.T, entries []pathQueryE2EPath, unexpectedPath string) {
	t.Helper()

	for _, entry := range entries {
		if entry.Path == unexpectedPath {
			t.Fatalf("did not expect path %q in entries %+v", unexpectedPath, pathQueryE2EPaths(entries))
		}
	}
}

func assertPathQueryE2EMatchedAVUPresent(t *testing.T, matched map[string][]pathQueryE2EAVU, irodsPath string, attrib string, value string, unit string) {
	t.Helper()

	requirePathQueryE2EMatchedAVU(t, matched, irodsPath, attrib, value, unit)
}

func requirePathQueryE2EMatchedAVU(t *testing.T, matched map[string][]pathQueryE2EAVU, irodsPath string, attrib string, value string, unit string) pathQueryE2EAVU {
	t.Helper()

	for _, avu := range matched[irodsPath] {
		if avu.Attrib == attrib && avu.Value == value && avu.Unit == unit {
			return avu
		}
	}
	t.Fatalf("expected matched AVU %s=%s[%s] for %q, got %+v", attrib, value, unit, irodsPath, matched[irodsPath])
	return pathQueryE2EAVU{}
}

func assertPathQueryE2EAVUHATEOAS(t *testing.T, avu pathQueryE2EAVU) {
	t.Helper()

	if strings.TrimSpace(avu.ID) == "" {
		t.Fatalf("expected matched AVU id to be populated: %+v", avu)
	}
	if avu.Links.Update.Method != http.MethodPut || strings.TrimSpace(avu.Links.Update.Href) == "" {
		t.Fatalf("expected matched AVU update link to be populated: %+v", avu.Links.Update)
	}
	if !strings.Contains(avu.Links.Update.Href, "/api/v1/path/avu/"+avu.ID) {
		t.Fatalf("expected matched AVU update link to contain id %q, got %q", avu.ID, avu.Links.Update.Href)
	}
	if avu.Links.Delete.Method != http.MethodDelete || strings.TrimSpace(avu.Links.Delete.Href) == "" {
		t.Fatalf("expected matched AVU delete link to be populated: %+v", avu.Links.Delete)
	}
}

func e2eHrefURL(baseURL string, href string) string {
	if strings.HasPrefix(href, "http://") || strings.HasPrefix(href, "https://") {
		return href
	}
	return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(href, "/")
}

func assertPathQueryE2EOmitsReplicas(t *testing.T, entries []pathQueryE2EPath, dataObjectPath string) {
	t.Helper()

	for _, entry := range entries {
		if entry.Path != dataObjectPath {
			continue
		}
		if entry.Kind != "data_object" {
			t.Fatalf("expected %q to be a data object, got %+v", dataObjectPath, entry)
		}
		if len(entry.Replicas) != 0 {
			t.Fatalf("expected data object %q to omit replicas by default, got %+v", dataObjectPath, entry.Replicas)
		}
		return
	}
	t.Fatalf("expected data object %q in entries %+v", dataObjectPath, pathQueryE2EPaths(entries))
}

func assertPathQueryE2EQueryString(t *testing.T, query map[string]json.RawMessage, field string, expected string) {
	t.Helper()

	raw, ok := query[field]
	if !ok {
		t.Fatalf("expected query summary field %q in %+v", field, query)
	}
	var actual string
	if err := json.Unmarshal(raw, &actual); err != nil {
		t.Fatalf("decode query summary field %q: %v", field, err)
	}
	if actual != expected {
		t.Fatalf("expected query summary %s=%q, got %q", field, expected, actual)
	}
}

func assertPathQueryE2EQueryScope(t *testing.T, query map[string]json.RawMessage, expectedRoot string, expectedMode string) {
	t.Helper()

	raw, ok := query["scope"]
	if !ok {
		t.Fatalf("expected query summary scope in %+v", query)
	}
	var scope struct {
		Root string `json:"root"`
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal(raw, &scope); err != nil {
		t.Fatalf("decode query summary scope: %v", err)
	}
	if scope.Root != expectedRoot || scope.Mode != expectedMode {
		t.Fatalf("expected query summary scope root=%q mode=%q, got %+v", expectedRoot, expectedMode, scope)
	}
}

func pathQueryE2EPaths(entries []pathQueryE2EPath) []string {
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if strings.TrimSpace(entry.Path) == "" {
			continue
		}
		paths = append(paths, entry.Path)
	}
	return paths
}
