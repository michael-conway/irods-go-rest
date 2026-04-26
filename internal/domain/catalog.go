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
	Parent       *ParentLink       `json:"parent,omitempty"`
	PathSegments []PathSegmentLink `json:"path_segments,omitempty"`
	Checksum     string            `json:"checksum,omitempty"`
	Size         int64             `json:"size,omitempty"`
	DisplaySize  string            `json:"display_size,omitempty"`
	Resource     string            `json:"resource,omitempty"`
	CreatedAt    *time.Time        `json:"created_at,omitempty"`
	UpdatedAt    *time.Time        `json:"updated_at,omitempty"`
	HasChildren  bool              `json:"hasChildren,omitempty"`
	ChildCount   int               `json:"childCount,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
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
	ContentType string
	Size        int64
	Reader      RangeReadCloser
}

type RangeReadCloser interface {
	io.ReaderAt
	io.Closer
}
