package domain

type PathEntry struct {
	ID          string            `json:"id"`
	Path        string            `json:"path"`
	Kind        string            `json:"kind"`
	Zone        string            `json:"zone"`
	Parent      *ParentLink       `json:"parent,omitempty"`
	Checksum    string            `json:"checksum,omitempty"`
	Size        int64             `json:"size,omitempty"`
	Resource    string            `json:"resource,omitempty"`
	HasChildren bool              `json:"hasChildren,omitempty"`
	ChildCount  int               `json:"childCount,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type ParentLink struct {
	IRODSPath string `json:"irods_path"`
	Href      string `json:"href"`
}

type ObjectContent struct {
	Path        string
	ContentType string
	Size        int64
	Data        []byte
}
