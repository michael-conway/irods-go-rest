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
	CmdCues      []CmdCue          `json:"cmd_cues,omitempty"`
	Links        *PathLinks        `json:"links,omitempty"`
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
	Number            int64             `json:"number"`
	Owner             string            `json:"owner,omitempty"`
	ResourceName      string            `json:"resource_name,omitempty"`
	ResourceLink      *ActionLink       `json:"resource_link,omitempty"`
	Links             *PathReplicaLinks `json:"links,omitempty"`
	ResourceHierarchy string            `json:"resource_hierarchy,omitempty"`
	Size              int64             `json:"size,omitempty"`
	DisplaySize       string            `json:"display_size,omitempty"`
	UpdatedAt         *time.Time        `json:"updated_at,omitempty"`
	Status            string            `json:"status,omitempty"`
	StatusSymbol      string            `json:"status_symbol,omitempty"`
	StatusDescription string            `json:"status_description,omitempty"`
	Checksum          string            `json:"checksum,omitempty"`
	DataType          string            `json:"data_type,omitempty"`
	PhysicalPath      string            `json:"physical_path,omitempty"`
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

type PathACL struct {
	IRODSPath          string            `json:"irods_path"`
	Kind               string            `json:"kind"`
	InheritanceEnabled *bool             `json:"inheritance_enabled,omitempty"`
	PathSegments       []PathSegmentLink `json:"path_segments,omitempty"`
	Links              *PathACLLinks     `json:"links,omitempty"`
	Users              []PathACLEntry    `json:"users"`
	Groups             []PathACLEntry    `json:"groups"`
}

type PathACLEntry struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Zone          string            `json:"zone,omitempty"`
	Type          string            `json:"type"`
	IRODSUserType string            `json:"irods_user_type,omitempty"`
	AccessLevel   string            `json:"access_level"`
	Links         *PathACLItemLinks `json:"links,omitempty"`
}

type PathLinks struct {
	Self                  *ActionLink `json:"self,omitempty"`
	Parent                *ActionLink `json:"parent,omitempty"`
	Children              *ActionLink `json:"children,omitempty"`
	Details               *ActionLink `json:"details,omitempty"`
	Update                *ActionLink `json:"update,omitempty"`
	Delete                *ActionLink `json:"delete,omitempty"`
	Relocate              *ActionLink `json:"relocate,omitempty"`
	Move                  *ActionLink `json:"move,omitempty"`
	Copy                  *ActionLink `json:"copy,omitempty"`
	UploadContents        *ActionLink `json:"upload_contents,omitempty"`
	ReplaceContents       *ActionLink `json:"replace_contents,omitempty"`
	DownloadContents      *ActionLink `json:"download_contents,omitempty"`
	Next                  *ActionLink `json:"next,omitempty"`
	Prev                  *ActionLink `json:"prev,omitempty"`
	AddReplica            *ActionLink `json:"add_replica,omitempty"`
	MoveReplica           *ActionLink `json:"move_replica,omitempty"`
	TrimReplica           *ActionLink `json:"trim_replica,omitempty"`
	AVUs                  *ActionLink `json:"avus,omitempty"`
	ACLs                  *ActionLink `json:"acls,omitempty"`
	Replicas              *ActionLink `json:"replicas,omitempty"`
	CreateAVU             *ActionLink `json:"create_avu,omitempty"`
	CreateTicket          *ActionLink `json:"create_ticket,omitempty"`
	Resources             *ActionLink `json:"resources,omitempty"`
	CreateChildCollection *ActionLink `json:"create_child_collection,omitempty"`
	CreateChildDataObject *ActionLink `json:"create_child_data_object,omitempty"`
	SetInheritance        *ActionLink `json:"set_inheritance,omitempty"`
	DeleteInheritance     *ActionLink `json:"delete_inheritance,omitempty"`
}

type PathChildrenLinks struct {
	Self                  *ActionLink `json:"self,omitempty"`
	Parent                *ActionLink `json:"parent,omitempty"`
	Next                  *ActionLink `json:"next,omitempty"`
	Prev                  *ActionLink `json:"prev,omitempty"`
	CreateChildCollection *ActionLink `json:"create_child_collection,omitempty"`
	CreateChildDataObject *ActionLink `json:"create_child_data_object,omitempty"`
	UploadContents        *ActionLink `json:"upload_contents,omitempty"`
}

type PathReplicasLinks struct {
	Self        *ActionLink `json:"self,omitempty"`
	AddReplica  *ActionLink `json:"add_replica,omitempty"`
	MoveReplica *ActionLink `json:"move_replica,omitempty"`
	TrimReplica *ActionLink `json:"trim_replica,omitempty"`
}

type PathReplicaLinks struct {
	Trim            *ActionLink `json:"trim,omitempty"`
	ResourceDetails *ActionLink `json:"resource_details,omitempty"`
}

type AVULinks struct {
	Update *ActionLink `json:"update,omitempty"`
	Delete *ActionLink `json:"delete,omitempty"`
}

type PathACLLinks struct {
	Path           *ActionLink `json:"path,omitempty"`
	AddUser        *ActionLink `json:"add_user,omitempty"`
	SetInheritance *ActionLink `json:"set_inheritance,omitempty"`
}

type PathACLItemLinks struct {
	Update *ActionLink `json:"update,omitempty"`
	Remove *ActionLink `json:"remove,omitempty"`
}

type ActionLink struct {
	Href   string `json:"href"`
	Method string `json:"method,omitempty"`
}

type CmdCue struct {
	Operation string `json:"operation,omitempty"`
	GoCmd     string `json:"gocmd,omitempty"`
	ICommand  string `json:"icommand,omitempty"`
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

type UploadChecksumInfo struct {
	Requested bool   `json:"requested"`
	Verified  bool   `json:"verified"`
	Algorithm string `json:"algorithm,omitempty"`
	Value     string `json:"value,omitempty"`
}

type PathContentsUploadLinks struct {
	Path     *ActionLink `json:"path,omitempty"`
	Contents *ActionLink `json:"contents,omitempty"`
	Parent   *ActionLink `json:"parent,omitempty"`
}

type PathContentsUploadResult struct {
	Path       string                   `json:"path"`
	ParentPath string                   `json:"parent_path"`
	FileName   string                   `json:"file_name"`
	Action     string                   `json:"action"`
	Size       int64                    `json:"size"`
	Checksum   *UploadChecksumInfo      `json:"checksum,omitempty"`
	Links      *PathContentsUploadLinks `json:"links,omitempty"`
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
