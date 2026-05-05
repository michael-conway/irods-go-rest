//go:build e2e
// +build e2e

package e2e

import (
	"crypto/rand"
	"fmt"
	"io/fs"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodslibfs "github.com/cyverse/go-irodsclient/irods/fs"
	irodstypes "github.com/cyverse/go-irodsclient/irods/types"
)

type e2eFixture struct {
	localRootPath       string
	irodsRootPath       string
	collectionPath      string
	objectPath          string
	childCollectionPath string
	missingPath         string
	objectAVU           e2eFixtureAVU
	collectionAVU       e2eFixtureAVU
}

type e2eFixtureAVU struct {
	Attrib string
	Value  string
	Unit   string
}

type generatedTreeManifest struct {
	objectRelPath          string
	childCollectionRelPath string
}

var (
	e2eFixtureOnce  sync.Once
	e2eFixtureValue *e2eFixture
	e2eFixtureErr   error
)

func requireE2EFixture(t *testing.T) *e2eFixture {
	t.Helper()

	e2eFixtureOnce.Do(func() {
		e2eFixtureValue, e2eFixtureErr = buildE2EFixture(t)
	})

	if e2eFixtureErr != nil {
		t.Fatalf("build e2e fixture: %v", e2eFixtureErr)
	}

	return e2eFixtureValue
}

func buildE2EFixture(t *testing.T) (*e2eFixture, error) {
	t.Helper()

	localRootPath, err := fixtureLocalRootPath()
	if err != nil {
		return nil, err
	}
	if err := os.RemoveAll(localRootPath); err != nil {
		return nil, fmt.Errorf("remove local fixture root %q: %w", localRootPath, err)
	}
	if err := os.MkdirAll(localRootPath, 0o755); err != nil {
		return nil, fmt.Errorf("create local fixture root %q: %w", localRootPath, err)
	}

	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	manifest, err := generateLocalFixtureTree(localRootPath, rng)
	if err != nil {
		return nil, err
	}

	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	irodsRootPath := fmt.Sprintf(
		"/%s/home/%s/e2e-fixture-%s",
		e2eIRODSZone(t),
		e2eBasicUsername(t),
		randomToken(rng, 8),
	)

	if err := filesystem.MakeDir(irodsRootPath, true); err != nil {
		return nil, fmt.Errorf("create iRODS fixture root %q: %w", irodsRootPath, err)
	}

	if err := uploadLocalFixtureTree(filesystem, localRootPath, irodsRootPath); err != nil {
		return nil, err
	}

	objectPath := irodsJoin(irodsRootPath, manifest.objectRelPath)
	collectionPath := irodsRootPath
	objectAVU := e2eFixtureAVU{
		Attrib: "e2e.object.avu",
		Value:  "present",
		Unit:   "fixture",
	}
	collectionAVU := e2eFixtureAVU{
		Attrib: "e2e.collection.avu",
		Value:  "root",
		Unit:   "fixture",
	}

	if err := filesystem.AddMetadata(objectPath, objectAVU.Attrib, objectAVU.Value, objectAVU.Unit); err != nil {
		return nil, fmt.Errorf("add object AVU to %q: %w", objectPath, err)
	}
	if err := filesystem.AddMetadata(collectionPath, collectionAVU.Attrib, collectionAVU.Value, collectionAVU.Unit); err != nil {
		return nil, fmt.Errorf("add collection AVU to %q: %w", collectionPath, err)
	}

	return &e2eFixture{
		localRootPath:       localRootPath,
		irodsRootPath:       irodsRootPath,
		collectionPath:      collectionPath,
		objectPath:          objectPath,
		childCollectionPath: irodsJoin(irodsRootPath, manifest.childCollectionRelPath),
		missingPath:         irodsJoin(irodsRootPath, "missing-"+randomToken(rng, 6)+".txt"),
		objectAVU:           objectAVU,
		collectionAVU:       collectionAVU,
	}, nil
}

func fixtureLocalRootPath() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve e2e fixture source path: runtime caller unavailable")
	}

	return filepath.Join(filepath.Dir(filename), "resources", "test_folder"), nil
}

func newE2EIRODSFilesystem(t *testing.T) *irodsfs.FileSystem {
	t.Helper()

	requestAuthScheme := irodstypes.GetAuthScheme(e2eIRODSAuthScheme(t))
	adminAuthScheme := requestAuthScheme
	cfg := optionalE2ERestConfig(t)
	if cfg != nil {
		requestAuthScheme = cfg.RequestAuthScheme()
		adminAuthScheme = cfg.AdminAuthScheme()
	}
	targetUser := e2eBasicUsername(t)
	uploaderUser := e2eIRODSUser(t)
	defaultResource := e2eIRODSDefaultResource(t)

	var (
		account *irodstypes.IRODSAccount
		err     error
	)

	if uploaderUser != "" && uploaderUser != targetUser {
		account, err = irodstypes.CreateIRODSProxyAccount(
			e2eIRODSHost(t),
			e2eIRODSPort(t),
			targetUser,
			e2eIRODSZone(t),
			uploaderUser,
			e2eIRODSZone(t),
			adminAuthScheme,
			e2eIRODSPassword(t),
			defaultResource,
		)
	} else {
		account, err = irodstypes.CreateIRODSAccount(
			e2eIRODSHost(t),
			e2eIRODSPort(t),
			targetUser,
			e2eIRODSZone(t),
			requestAuthScheme,
			e2eIRODSPassword(t),
			defaultResource,
		)
	}

	if err != nil {
		t.Fatalf("create iRODS account: %v", err)
	}
	if cfg != nil {
		cfg.ApplyIRODSConnectionConfig(account)
	}

	filesystem, err := irodsfs.NewFileSystemWithDefault(account, "irods-go-rest-e2e-fixture")
	if err != nil {
		t.Fatalf("connect to iRODS for E2E fixture setup: %v", err)
	}

	return filesystem
}

func generateLocalFixtureTree(rootPath string, rng *mathrand.Rand) (*generatedTreeManifest, error) {
	manifest := &generatedTreeManifest{}

	if err := generateLocalFixtureCollection(rootPath, "", 0, 4, rng, manifest); err != nil {
		return nil, err
	}

	if manifest.objectRelPath == "" {
		return nil, fmt.Errorf("generated fixture tree missing object path")
	}
	if manifest.childCollectionRelPath == "" {
		return nil, fmt.Errorf("generated fixture tree missing child collection path")
	}

	return manifest, nil
}

func generateLocalFixtureCollection(localDir string, relDir string, depth int, maxDepth int, rng *mathrand.Rand, manifest *generatedTreeManifest) error {
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return fmt.Errorf("create local fixture collection %q: %w", localDir, err)
	}

	fileCount := 8 + rng.Intn(5)
	for i := 0; i < fileCount; i++ {
		fileName := generatedFileName(rng)
		filePath := filepath.Join(localDir, fileName)
		size := 1 + rng.Intn(100)
		payload := make([]byte, size)
		if _, err := rand.Read(payload); err != nil {
			return fmt.Errorf("generate random payload for %q: %w", filePath, err)
		}
		if err := os.WriteFile(filePath, payload, 0o644); err != nil {
			return fmt.Errorf("write local fixture file %q: %w", filePath, err)
		}

		if manifest.objectRelPath == "" {
			manifest.objectRelPath = filepath.ToSlash(filepath.Join(relDir, fileName))
		}
	}

	if depth >= maxDepth {
		return nil
	}

	childCollectionCount := 2
	for i := 0; i < childCollectionCount; i++ {
		childName := generatedCollectionName(rng)
		childRelDir := filepath.Join(relDir, childName)
		childLocalDir := filepath.Join(localDir, childName)

		if manifest.childCollectionRelPath == "" && depth == 0 {
			manifest.childCollectionRelPath = filepath.ToSlash(childRelDir)
		}

		if err := generateLocalFixtureCollection(childLocalDir, childRelDir, depth+1, maxDepth, rng, manifest); err != nil {
			return err
		}
	}

	return nil
}

func uploadLocalFixtureTree(filesystem *irodsfs.FileSystem, localRootPath string, irodsRootPath string) error {
	return filepath.WalkDir(localRootPath, func(localPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if localPath == localRootPath {
			return nil
		}

		relPath, err := filepath.Rel(localRootPath, localPath)
		if err != nil {
			return fmt.Errorf("derive relative path for %q: %w", localPath, err)
		}

		irodsPath := irodsJoin(irodsRootPath, filepath.ToSlash(relPath))
		if d.IsDir() {
			if err := filesystem.MakeDir(irodsPath, true); err != nil {
				return fmt.Errorf("create iRODS fixture collection %q: %w", irodsPath, err)
			}
			return nil
		}

		if _, err := filesystem.UploadFile(localPath, irodsPath, "", false, false, nil); err != nil {
			return fmt.Errorf("upload fixture file %q to %q: %w", localPath, irodsPath, err)
		}

		return nil
	})
}

func requireE2EChecksum(t *testing.T, irodsPath string) {
	t.Helper()

	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	conn, err := filesystem.GetMetadataConnection(false)
	if err != nil {
		t.Fatalf("get metadata connection for %q: %v", irodsPath, err)
	}
	defer filesystem.ReturnMetadataConnection(conn) //nolint:errcheck

	checksum, err := irodslibfs.GetDataObjectChecksum(conn, irodsPath, "")
	if err != nil {
		t.Fatalf("compute checksum for %q: %v", irodsPath, err)
	}
	if checksum == nil || strings.TrimSpace(checksum.IRODSChecksumString) == "" {
		t.Fatalf("expected computed checksum for %q to be populated", irodsPath)
	}
}

func requireE2EChecksummedObjectPath(t *testing.T, fixture *e2eFixture) string {
	t.Helper()

	if fixture == nil {
		t.Fatal("expected fixture to be populated")
	}

	filesystem := newE2EIRODSFilesystem(t)
	defer filesystem.Release()

	destPath := irodsJoin(
		fixture.irodsRootPath,
		"checksummed-"+randomToken(mathrand.New(mathrand.NewSource(time.Now().UnixNano())), 8)+filepath.Ext(fixture.objectPath),
	)

	if err := filesystem.CopyFile(fixture.objectPath, destPath, true); err != nil {
		t.Fatalf("copy %q to %q for checksum setup: %v", fixture.objectPath, destPath, err)
	}

	requireE2EChecksum(t, destPath)
	return destPath
}

func generatedCollectionName(rng *mathrand.Rand) string {
	return "collection_" + randomToken(rng, 10)
}

func generatedFileName(rng *mathrand.Rand) string {
	extensions := []string{".txt", ".md", ".json", ".csv", ".tsv", ".yaml", ".xml", ".log", ".html", ".dat"}
	return "file_" + randomToken(rng, 12) + extensions[rng.Intn(len(extensions))]
}

func randomToken(rng *mathrand.Rand, length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	if rng == nil {
		rng = mathrand.New(mathrand.NewSource(time.Now().UnixNano()))
	}

	builder := strings.Builder{}
	builder.Grow(length)
	for i := 0; i < length; i++ {
		builder.WriteByte(alphabet[rng.Intn(len(alphabet))])
	}

	return builder.String()
}

func irodsJoin(base string, rel string) string {
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return base
	}

	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(filepath.ToSlash(rel), "/")
}
