package irods

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	irodsuriext "github.com/michael-conway/go-irodsclient-extensions/irodsuri"
	metadataext "github.com/michael-conway/go-irodsclient-extensions/metadata"
)

func (s *catalogService) GetMetadataManifest(_ context.Context, requestContext *RequestContext, absolutePath string) (metadataext.Manifest, error) {
	absolutePath = strings.TrimSpace(absolutePath)
	if absolutePath == "" {
		return metadataext.Manifest{}, fmt.Errorf("%w: path %q", ErrNotFound, absolutePath)
	}

	filesystem, err := s.filesystemForRequest(requestContext, "irods-go-rest-get-metadata-manifest")
	if err != nil {
		return metadataext.Manifest{}, err
	}
	defer filesystem.Release()

	service, err := metadataext.NewService(&metadataManifestFilesystem{
		filesystem: filesystem,
		host:       strings.TrimSpace(s.cfg.IrodsHost),
		port:       s.cfg.IrodsPort,
		zone:       strings.TrimSpace(s.cfg.IrodsZone),
	})
	if err != nil {
		return metadataext.Manifest{}, err
	}

	manifest, err := service.GenerateManifest(absolutePath)
	if err != nil {
		return metadataext.Manifest{}, normalizePathAccessError("generate metadata manifest", absolutePath, err)
	}
	return manifest, nil
}

type metadataManifestFilesystem struct {
	filesystem CatalogFileSystem
	host       string
	port       int
	zone       string
}

func (mfs *metadataManifestFilesystem) Stat(irodsPath string) (*metadataext.PathStat, error) {
	entry, err := mfs.filesystem.Stat(irodsPath)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}

	pathStat := &metadataext.PathStat{
		ID:           entry.ID,
		Name:         entry.Name,
		Path:         entry.Path,
		Owner:        entry.Owner,
		Size:         entry.Size,
		DataType:     entry.DataType,
		CreateTime:   entry.CreateTime,
		ModifyTime:   entry.ModifyTime,
		AccessTime:   entry.AccessTime,
		IsCollection: entry.IsDir(),
	}
	if entry.CheckSumAlgorithm != "" {
		pathStat.ChecksumAlgorithm = string(entry.CheckSumAlgorithm)
	}
	if len(entry.CheckSum) > 0 {
		pathStat.Checksum = hex.EncodeToString(entry.CheckSum)
	}

	replicas := make([]metadataext.ReplicaStat, 0, len(entry.IRODSReplicas))
	for _, replica := range entry.IRODSReplicas {
		replicaStat := metadataext.ReplicaStat{
			Number:            replica.Number,
			Owner:             replica.Owner,
			Status:            replica.Status,
			ResourceName:      replica.ResourceName,
			ResourceHierarchy: replica.ResourceHierarchy,
			Path:              replica.Path,
			CreateTime:        replica.CreateTime,
			ModifyTime:        replica.ModifyTime,
			AccessTime:        replica.AccessTime,
		}
		if replica.Checksum != nil {
			replicaStat.ChecksumAlgorithm = string(replica.Checksum.Algorithm)
			if len(replica.Checksum.Checksum) > 0 {
				replicaStat.Checksum = hex.EncodeToString(replica.Checksum.Checksum)
			}
		}
		replicas = append(replicas, replicaStat)
	}
	pathStat.Replicas = replicas

	return pathStat, nil
}

func (mfs *metadataManifestFilesystem) ListMetadata(irodsPath string) ([]metadataext.AVUStat, error) {
	metadata, err := mfs.filesystem.ListMetadata(irodsPath)
	if err != nil {
		return nil, err
	}

	result := make([]metadataext.AVUStat, 0, len(metadata))
	for _, avu := range metadata {
		if avu == nil {
			continue
		}

		result = append(result, metadataext.AVUStat{
			Name:       avu.Name,
			Value:      avu.Value,
			Units:      avu.Units,
			CreateTime: avu.CreateTime,
			ModifyTime: avu.ModifyTime,
		})
	}
	return result, nil
}

func (mfs *metadataManifestFilesystem) ConnectionInfoForPath(irodsPath string) (metadataext.ConnectionInfo, error) {
	zone := strings.TrimSpace(mfs.zone)
	if zone == "" {
		zone = zoneFromIRODSPath(irodsPath)
	}

	connectionInfo := metadataext.ConnectionInfo{
		Host: mfs.host,
		Port: mfs.port,
		Zone: zone,
	}

	if strings.TrimSpace(mfs.host) != "" && mfs.port > 0 {
		uri, err := irodsuriext.BuildAnonymous(mfs.host, mfs.port, irodsPath)
		if err == nil && uri != nil {
			connectionInfo.IRODSURI = uri.String()
		}
	}

	return connectionInfo, nil
}

func zoneFromIRODSPath(irodsPath string) string {
	irodsPath = strings.TrimSpace(irodsPath)
	if !strings.HasPrefix(irodsPath, "/") {
		return ""
	}

	trimmed := strings.TrimPrefix(irodsPath, "/")
	if trimmed == "" {
		return ""
	}

	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}
