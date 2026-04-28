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
	ResourceLink *ActionLink       `json:"resource_link,omitempty"`
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
	Number            int64       `json:"number"`
	Owner             string      `json:"owner,omitempty"`
	ResourceName      string      `json:"resource_name,omitempty"`
	ResourceLink      *ActionLink `json:"resource_link,omitempty"`
	ResourceHierarchy string      `json:"resource_hierarchy,omitempty"`
	Size              int64       `json:"size,omitempty"`
	DisplaySize       string      `json:"display_size,omitempty"`
	UpdatedAt         *time.Time  `json:"updated_at,omitempty"`
	Status            string      `json:"status,omitempty"`
	StatusSymbol      string      `json:"status_symbol,omitempty"`
	StatusDescription string      `json:"status_description,omitempty"`
	Checksum          string      `json:"checksum,omitempty"`
	DataType          string      `json:"data_type,omitempty"`
	PhysicalPath      string      `json:"physical_path,omitempty"`
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
	AVUs                  *ActionLink `json:"avus,omitempty"`
	CreateAVU             *ActionLink `json:"create_avu,omitempty"`
	CreateTicket          *ActionLink `json:"create_ticket,omitempty"`
	Resources             *ActionLink `json:"resources,omitempty"`
	CreateChildCollection *ActionLink `json:"create_child_collection,omitempty"`
	CreateChildDataObject *ActionLink `json:"create_child_data_object,omitempty"`
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

type Ticket struct {
	Name           string       `json:"name"`
	BearerToken    string       `json:"bearer_token,omitempty"`
	Type           string       `json:"type,omitempty"`
	Owner          string       `json:"owner,omitempty"`
	OwnerZone      string       `json:"owner_zone,omitempty"`
	ObjectType     string       `json:"object_type,omitempty"`
	Path           string       `json:"irods_path,omitempty"`
	UsesLimit      int64        `json:"uses_limit,omitempty"`
	UsesCount      int64        `json:"uses_count,omitempty"`
	WriteFileLimit int64        `json:"write_file_limit,omitempty"`
	WriteFileCount int64        `json:"write_file_count,omitempty"`
	WriteByteLimit int64        `json:"write_byte_limit,omitempty"`
	WriteByteCount int64        `json:"write_byte_count,omitempty"`
	ExpirationTime *time.Time   `json:"expiration_time,omitempty"`
	Links          *TicketLinks `json:"links,omitempty"`
}

type TicketLinks struct {
	Self     *ActionLink `json:"self,omitempty"`
	Update   *ActionLink `json:"update,omitempty"`
	Delete   *ActionLink `json:"delete,omitempty"`
	Path     *ActionLink `json:"path,omitempty"`
	Download *ActionLink `json:"download,omitempty"`
}
