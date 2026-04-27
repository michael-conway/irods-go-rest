package domain

import (
	"io"
	"time"
)

type PathEntry struct {
	ID           string            `json:"id"`
	Path         string            `json:"path"`
	Kind         string            `json:"kind"`
	Zone         string            `json:"zone"`
	Links        *PathLinks        `json:"links,omitempty"`
	Parent       *ParentLink       `json:"parent,omitempty"`
	PathSegments []PathSegmentLink `json:"path_segments,omitempty"`
	Checksum     *PathChecksum     `json:"checksum,omitempty"`
	MimeType     string            `json:"mime_type,omitempty"`
	Size         int64             `json:"size,omitempty"`
	DisplaySize  string            `json:"display_size,omitempty"`
	Resource     string            `json:"resource,omitempty"`
	CreatedAt    *time.Time        `json:"created_at,omitempty"`
	UpdatedAt    *time.Time        `json:"updated_at,omitempty"`
	Replicas     []PathReplica     `json:"replicas,omitempty"`
	HasChildren  bool              `json:"hasChildren,omitempty"`
	ChildCount   int               `json:"childCount,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

type PathChecksum struct {
	Checksum string `json:"checksum,omitempty"`
	Type     string `json:"type,omitempty"`
}

type PathReplica struct {
	Number            int64      `json:"number"`
	Owner             string     `json:"owner,omitempty"`
	ResourceName      string     `json:"resource_name,omitempty"`
	ResourceHierarchy string     `json:"resource_hierarchy,omitempty"`
	Size              int64      `json:"size,omitempty"`
	DisplaySize       string     `json:"display_size,omitempty"`
	UpdatedAt         *time.Time `json:"updated_at,omitempty"`
	Status            string     `json:"status,omitempty"`
	StatusSymbol      string     `json:"status_symbol,omitempty"`
	StatusDescription string     `json:"status_description,omitempty"`
	Checksum          string     `json:"checksum,omitempty"`
	DataType          string     `json:"data_type,omitempty"`
	PhysicalPath      string     `json:"physical_path,omitempty"`
}

type AVUMetadata struct {
	ID        string     `json:"id"`
	Attrib    string     `json:"attrib"`
	Value     string     `json:"value"`
	Unit      string     `json:"unit,omitempty"`
	CreatedAt *time.Time `json:"created_at,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
	Links     *AVULinks  `json:"links,omitempty"`
}

type PathLinks struct {
	AVUs      *ActionLink `json:"avus,omitempty"`
	CreateAVU *ActionLink `json:"create_avu,omitempty"`
}

type AVULinks struct {
	Update *ActionLink `json:"update,omitempty"`
	Delete *ActionLink `json:"delete,omitempty"`
}

type ActionLink struct {
	Href   string `json:"href"`
	Method string `json:"method,omitempty"`
}

type ParentLink struct {
	IRODSPath string `json:"irods_path"`
	Href      string `json:"href"`
}

type PathSegmentLink struct {
	DisplayName string `json:"display_name"`
	IRODSPath   string `json:"irods_path"`
	Href        string `json:"href"`
}

type ObjectContent struct {
	Path        string
	FileName    string
	ContentType string
	Size        int64
	Checksum    *PathChecksum
	UpdatedAt   *time.Time
	Reader      RangeReadCloser
}

type RangeReadCloser interface {
	io.ReaderAt
	io.Closer
}
