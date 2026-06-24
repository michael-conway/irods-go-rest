package irods

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	metadataext "github.com/michael-conway/go-irodsclient-extensions/metadata"
	"github.com/michael-conway/irods-go-rest/internal/domain"
)

type principalMetadataFilesystemFunc func(requestContext *RequestContext, name string, zone string, applicationName string) (CatalogFileSystem, string, string, error)
type principalMetadataErrorFunc func(operation string, name string, zone string, err error) error

func listPrincipalMetadata(
	requestContext *RequestContext,
	name string,
	zone string,
	applicationName string,
	filesystemForPrincipal principalMetadataFilesystemFunc,
	normalize principalMetadataErrorFunc,
) ([]domain.AVUMetadata, error) {
	filesystem, name, zone, err := filesystemForPrincipal(requestContext, name, zone, applicationName)
	if err != nil {
		return nil, err
	}
	defer filesystem.Release()

	metadata, err := filesystem.ListUserMetadata(name, zone)
	if err != nil {
		return nil, normalize("list principal metadata", name, zone, err)
	}
	return avuMetadataList(metadata), nil
}

func addPrincipalMetadata(
	requestContext *RequestContext,
	name string,
	zone string,
	attrib string,
	value string,
	unit string,
	applicationName string,
	filesystemForPrincipal principalMetadataFilesystemFunc,
	normalize principalMetadataErrorFunc,
) (domain.AVUMetadata, error) {
	attrib, value, unit = normalizeAVUInputs(attrib, value, unit)
	if attrib == "" || value == "" {
		return domain.AVUMetadata{}, newInvalidRequestError("attrib and value are required")
	}

	filesystem, name, zone, err := filesystemForPrincipal(requestContext, name, zone, applicationName)
	if err != nil {
		return domain.AVUMetadata{}, err
	}
	defer filesystem.Release()

	if err := filesystem.AddUserMetadata(name, zone, attrib, value, unit); err != nil {
		return domain.AVUMetadata{}, normalize("add principal metadata", name, zone, err)
	}
	created, err := waitForLatestUserAVUMetadata(filesystem, name, zone, attrib, value, unit)
	if err != nil {
		return domain.AVUMetadata{}, normalize("list principal metadata", name, zone, err)
	}
	return created, nil
}

func updatePrincipalMetadata(
	requestContext *RequestContext,
	name string,
	zone string,
	avuID string,
	attrib string,
	value string,
	unit string,
	applicationName string,
	filesystemForPrincipal principalMetadataFilesystemFunc,
	normalize principalMetadataErrorFunc,
	notFoundTarget string,
) (domain.AVUMetadata, error) {
	avuIDInt, attrib, value, unit, err := principalMetadataUpdateInputs(avuID, attrib, value, unit)
	if err != nil {
		return domain.AVUMetadata{}, err
	}

	filesystem, name, zone, err := filesystemForPrincipal(requestContext, name, zone, applicationName)
	if err != nil {
		return domain.AVUMetadata{}, err
	}
	defer filesystem.Release()

	target := metadataext.AVUStat{Name: attrib, Value: value, Units: unit}
	updatedStat, err := filesystem.ReplaceUserMetadataByID(name, zone, avuIDInt, target)
	if err != nil {
		if errors.Is(err, metadataext.ErrAVUNotFound) {
			return domain.AVUMetadata{}, fmt.Errorf("%w: avu %q on %s %q", ErrNotFound, avuID, notFoundTarget, name)
		}
		return domain.AVUMetadata{}, normalize("replace principal metadata", name, zone, err)
	}
	if updatedStat.ID > 0 {
		return avuMetadataFromStat(updatedStat), nil
	}
	return waitForLatestUserAVUMetadata(filesystem, name, zone, attrib, value, unit)
}

func deletePrincipalMetadata(
	requestContext *RequestContext,
	name string,
	zone string,
	avuID string,
	applicationName string,
	filesystemForPrincipal principalMetadataFilesystemFunc,
	normalize principalMetadataErrorFunc,
	notFoundTarget string,
) error {
	avuIDInt, err := principalMetadataID(avuID)
	if err != nil {
		return err
	}

	filesystem, name, zone, err := filesystemForPrincipal(requestContext, name, zone, applicationName)
	if err != nil {
		return err
	}
	defer filesystem.Release()

	metadata, err := filesystem.ListUserMetadata(name, zone)
	if err != nil {
		return normalize("list principal metadata", name, zone, err)
	}
	if _, ok := findAVUMetadataByID(metadata, avuID); !ok {
		return fmt.Errorf("%w: avu %q on %s %q", ErrNotFound, avuID, notFoundTarget, name)
	}
	if err := filesystem.DeleteUserMetadata(name, zone, avuIDInt); err != nil {
		return normalize("delete principal metadata", name, zone, err)
	}
	return nil
}

func principalMetadataUpdateInputs(avuID string, attrib string, value string, unit string) (int64, string, string, string, error) {
	attrib, value, unit = normalizeAVUInputs(attrib, value, unit)
	if attrib == "" || value == "" {
		return 0, "", "", "", newInvalidRequestError("attrib and value are required")
	}
	avuIDInt, err := principalMetadataID(avuID)
	if err != nil {
		return 0, "", "", "", err
	}
	return avuIDInt, attrib, value, unit, nil
}

func principalMetadataID(avuID string) (int64, error) {
	avuID = strings.TrimSpace(avuID)
	if avuID == "" {
		return 0, newInvalidRequestError("avu_id is required")
	}
	avuIDInt, err := strconv.ParseInt(avuID, 10, 64)
	if err != nil || avuIDInt <= 0 {
		return 0, newInvalidRequestErrorf("invalid avu id %q", avuID)
	}
	return avuIDInt, nil
}
