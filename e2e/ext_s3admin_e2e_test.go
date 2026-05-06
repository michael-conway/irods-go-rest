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

func TestS3AdminBucketsLifecycleBasicAuthE2E(t *testing.T) {
	baseURL := requireE2EBaseURL(t)
	fixture := requireE2EFixture(t)
	client := newE2EHTTPClient()
	bucketsURL := strings.TrimRight(baseURL, "/") + "/api/v1/ext/s3/buckets"

	nestedCollectionPath := createE2ES3AdminCollection(t, fixture.childCollectionPath)
	rootBucketID := "e2e-s3-" + randomToken(nil, 10)
	renamedBucketID := "e2e-s3-renamed-" + randomToken(nil, 10)
	createdBucketIDs := map[string]struct{}{}

	trackBucketID := func(bucketID string) {
		bucketID = strings.TrimSpace(bucketID)
		if bucketID != "" {
			createdBucketIDs[bucketID] = struct{}{}
		}
	}
	t.Cleanup(func() {
		for bucketID := range createdBucketIDs {
			status, body := requestS3AdminE2E(t, client, http.MethodDelete, bucketsURL+"/"+url.PathEscape(bucketID), nil)
			if status != http.StatusNoContent && status != http.StatusNotFound && status != http.StatusServiceUnavailable {
				t.Logf("cleanup delete S3 bucket %q returned %d: %s", bucketID, status, strings.TrimSpace(body))
			}
		}
	})

	createStatus, createBody := requestS3AdminE2E(t, client, http.MethodPost, bucketsURL, map[string]any{
		"bucket_name": rootBucketID,
		"irods_path":  fixture.collectionPath,
	})
	if createStatus == http.StatusNotFound || createStatus == http.StatusMethodNotAllowed || createStatus == http.StatusNotImplemented {
		t.Skipf("s3admin extension endpoint not available yet (status=%d)", createStatus)
	}
	if createStatus == http.StatusServiceUnavailable {
		t.Skipf("s3admin extension endpoint not configured: %s", strings.TrimSpace(createBody))
	}
	if createStatus != http.StatusCreated {
		t.Fatalf("expected 201 creating S3 bucket, got %d: %s", createStatus, strings.TrimSpace(createBody))
	}
	createdRootBucket := decodeS3BucketResponseE2E(t, createBody)
	if createdRootBucket.BucketID != rootBucketID || createdRootBucket.IRODSPath != fixture.collectionPath {
		t.Fatalf("expected created bucket %q at %q, got %+v", rootBucketID, fixture.collectionPath, createdRootBucket)
	}
	trackBucketID(rootBucketID)

	autoStatus, autoBody := requestS3AdminE2E(t, client, http.MethodPost, bucketsURL, map[string]any{
		"auto_generate": true,
		"irods_path":    nestedCollectionPath,
	})
	if autoStatus != http.StatusCreated {
		t.Fatalf("expected 201 creating auto-generated S3 bucket, got %d: %s", autoStatus, strings.TrimSpace(autoBody))
	}
	createdAutoBucket := decodeS3BucketResponseE2E(t, autoBody)
	if strings.TrimSpace(createdAutoBucket.BucketID) == "" || createdAutoBucket.IRODSPath != nestedCollectionPath {
		t.Fatalf("expected auto-generated bucket at %q, got %+v", nestedCollectionPath, createdAutoBucket)
	}
	trackBucketID(createdAutoBucket.BucketID)

	nonRecursiveURL := bucketsURL + "?irods_path=" + url.QueryEscape(fixture.collectionPath) + "&recursive=false"
	nonRecursiveStatus, nonRecursiveBody := requestS3AdminE2E(t, client, http.MethodGet, nonRecursiveURL, nil)
	if nonRecursiveStatus != http.StatusOK {
		t.Fatalf("expected 200 listing non-recursive S3 buckets, got %d: %s", nonRecursiveStatus, strings.TrimSpace(nonRecursiveBody))
	}
	nonRecursiveBuckets := decodeS3BucketListResponseE2E(t, nonRecursiveBody)
	assertS3BucketPresentE2E(t, nonRecursiveBuckets, rootBucketID, fixture.collectionPath)
	assertS3BucketAbsentE2E(t, nonRecursiveBuckets, createdAutoBucket.BucketID, nestedCollectionPath)

	recursiveURL := bucketsURL + "?irods_path=" + url.QueryEscape(fixture.collectionPath) + "&recursive=true"
	recursiveStatus, recursiveBody := requestS3AdminE2E(t, client, http.MethodGet, recursiveURL, nil)
	if recursiveStatus != http.StatusOK {
		t.Fatalf("expected 200 listing recursive S3 buckets, got %d: %s", recursiveStatus, strings.TrimSpace(recursiveBody))
	}
	recursiveBuckets := decodeS3BucketListResponseE2E(t, recursiveBody)
	assertS3BucketPresentE2E(t, recursiveBuckets, rootBucketID, fixture.collectionPath)
	assertS3BucketPresentE2E(t, recursiveBuckets, createdAutoBucket.BucketID, nestedCollectionPath)

	filteredURL := bucketsURL + "?irods_path=" + url.QueryEscape(fixture.collectionPath) + "&recursive=true&bucket_name=" + url.QueryEscape(rootBucketID)
	filteredStatus, filteredBody := requestS3AdminE2E(t, client, http.MethodGet, filteredURL, nil)
	if filteredStatus != http.StatusOK {
		t.Fatalf("expected 200 filtering S3 buckets by bucket_name, got %d: %s", filteredStatus, strings.TrimSpace(filteredBody))
	}
	filteredBuckets := decodeS3BucketListResponseE2E(t, filteredBody)
	assertS3BucketPresentE2E(t, filteredBuckets, rootBucketID, fixture.collectionPath)
	assertS3BucketAbsentE2E(t, filteredBuckets, createdAutoBucket.BucketID, nestedCollectionPath)

	getStatus, getBody := requestS3AdminE2E(t, client, http.MethodGet, bucketsURL+"/"+url.PathEscape(rootBucketID)+"?irods_path="+url.QueryEscape(fixture.collectionPath)+"&recursive=true", nil)
	if getStatus != http.StatusOK {
		t.Fatalf("expected 200 getting S3 bucket by id, got %d: %s", getStatus, strings.TrimSpace(getBody))
	}
	assertS3BucketEqualE2E(t, decodeS3BucketResponseE2E(t, getBody), rootBucketID, fixture.collectionPath)

	getByPathURL := bucketsURL + "/by-path?irods_path=" + url.QueryEscape(fixture.collectionPath)
	getByPathStatus, getByPathBody := requestS3AdminE2E(t, client, http.MethodGet, getByPathURL, nil)
	if getByPathStatus != http.StatusOK {
		t.Fatalf("expected 200 getting S3 bucket by path, got %d: %s", getByPathStatus, strings.TrimSpace(getByPathBody))
	}
	assertS3BucketEqualE2E(t, decodeS3BucketResponseE2E(t, getByPathBody), rootBucketID, fixture.collectionPath)

	updateStatus, updateBody := requestS3AdminE2E(t, client, http.MethodPut, bucketsURL, map[string]any{
		"bucket_name": renamedBucketID,
		"irods_path":  fixture.collectionPath,
	})
	if updateStatus != http.StatusOK {
		t.Fatalf("expected 200 updating S3 bucket name, got %d: %s", updateStatus, strings.TrimSpace(updateBody))
	}
	assertS3BucketEqualE2E(t, decodeS3BucketResponseE2E(t, updateBody), renamedBucketID, fixture.collectionPath)
	trackBucketID(renamedBucketID)

	duplicateStatus, duplicateBody := requestS3AdminE2E(t, client, http.MethodPost, bucketsURL, map[string]any{
		"bucket_name": renamedBucketID,
		"irods_path":  nestedCollectionPath,
	})
	if duplicateStatus != http.StatusConflict {
		t.Fatalf("expected 409 creating duplicate S3 bucket name, got %d: %s", duplicateStatus, strings.TrimSpace(duplicateBody))
	}

	refreshStatus, refreshBody := requestS3AdminE2E(t, client, http.MethodPost, bucketsURL+"/refresh-mapping", nil)
	if refreshStatus != http.StatusOK {
		t.Fatalf("expected 200 refreshing S3 bucket mapping, got %d: %s", refreshStatus, strings.TrimSpace(refreshBody))
	}
	refreshedBuckets := decodeS3BucketMappingRefreshResponseE2E(t, refreshBody)
	assertS3BucketPresentE2E(t, refreshedBuckets, renamedBucketID, fixture.collectionPath)
	assertS3BucketPresentE2E(t, refreshedBuckets, createdAutoBucket.BucketID, nestedCollectionPath)

	deleteStatus, deleteBody := requestS3AdminE2E(t, client, http.MethodDelete, bucketsURL+"/"+url.PathEscape(renamedBucketID), nil)
	if deleteStatus != http.StatusNoContent {
		t.Fatalf("expected 204 deleting renamed S3 bucket, got %d: %s", deleteStatus, strings.TrimSpace(deleteBody))
	}

	afterDeleteStatus, afterDeleteBody := requestS3AdminE2E(t, client, http.MethodGet, bucketsURL+"/"+url.PathEscape(renamedBucketID), nil)
	if afterDeleteStatus != http.StatusNotFound {
		t.Fatalf("expected 404 getting deleted S3 bucket, got %d: %s", afterDeleteStatus, strings.TrimSpace(afterDeleteBody))
	}
}

type s3BucketE2E struct {
	BucketID  string `json:"bucket_id"`
	IRODSPath string `json:"irods_path"`
}

func createE2ES3AdminCollection(t *testing.T, parentPath string) string {
	t.Helper()

	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	collectionPath := irodsJoin(parentPath, "s3admin-e2e-"+randomToken(nil, 8))
	if err := filesystem.MakeDir(collectionPath, true); err != nil {
		t.Fatalf("create S3 admin E2E collection %q: %v", collectionPath, err)
	}
	return collectionPath
}

func requestS3AdminE2E(t *testing.T, client *http.Client, method string, requestURL string, payload any) (int, string) {
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

func decodeS3BucketResponseE2E(t *testing.T, responseBody string) s3BucketE2E {
	t.Helper()

	var payload struct {
		Bucket s3BucketE2E `json:"bucket"`
	}
	if err := json.Unmarshal([]byte(responseBody), &payload); err != nil {
		t.Fatalf("decode S3 bucket response: %v", err)
	}
	return payload.Bucket
}

func decodeS3BucketListResponseE2E(t *testing.T, responseBody string) []s3BucketE2E {
	t.Helper()

	var payload struct {
		Buckets []s3BucketE2E `json:"buckets"`
		Count   int           `json:"count"`
	}
	if err := json.Unmarshal([]byte(responseBody), &payload); err != nil {
		t.Fatalf("decode S3 bucket list response: %v", err)
	}
	if payload.Count != len(payload.Buckets) {
		t.Fatalf("expected S3 bucket count %d to match list length %d: %s", payload.Count, len(payload.Buckets), responseBody)
	}
	return payload.Buckets
}

func decodeS3BucketMappingRefreshResponseE2E(t *testing.T, responseBody string) []s3BucketE2E {
	t.Helper()

	var payload struct {
		BucketMapping struct {
			Buckets []s3BucketE2E `json:"buckets"`
			Count   int           `json:"count"`
		} `json:"bucket_mapping"`
	}
	if err := json.Unmarshal([]byte(responseBody), &payload); err != nil {
		t.Fatalf("decode S3 bucket mapping refresh response: %v", err)
	}
	if payload.BucketMapping.Count != len(payload.BucketMapping.Buckets) {
		t.Fatalf("expected S3 bucket refresh count %d to match list length %d: %s", payload.BucketMapping.Count, len(payload.BucketMapping.Buckets), responseBody)
	}
	return payload.BucketMapping.Buckets
}

func assertS3BucketEqualE2E(t *testing.T, bucket s3BucketE2E, expectedBucketID string, expectedPath string) {
	t.Helper()

	if bucket.BucketID != expectedBucketID || bucket.IRODSPath != expectedPath {
		t.Fatalf("expected S3 bucket %q at %q, got %+v", expectedBucketID, expectedPath, bucket)
	}
}

func assertS3BucketPresentE2E(t *testing.T, buckets []s3BucketE2E, expectedBucketID string, expectedPath string) {
	t.Helper()

	for _, bucket := range buckets {
		if bucket.BucketID == expectedBucketID && bucket.IRODSPath == expectedPath {
			return
		}
	}
	t.Fatalf("expected S3 bucket %q at %q in response: %+v", expectedBucketID, expectedPath, buckets)
}

func assertS3BucketAbsentE2E(t *testing.T, buckets []s3BucketE2E, unexpectedBucketID string, unexpectedPath string) {
	t.Helper()

	for _, bucket := range buckets {
		if bucket.BucketID == unexpectedBucketID && bucket.IRODSPath == unexpectedPath {
			t.Fatalf("did not expect S3 bucket %q at %q in response: %+v", unexpectedBucketID, unexpectedPath, buckets)
		}
	}
}
