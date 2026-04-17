package domain

type Object struct {
	ID       string            `json:"id"`
	Path     string            `json:"path"`
	Checksum string            `json:"checksum"`
	Size     int64             `json:"size"`
	Zone     string            `json:"zone"`
	Resource string            `json:"resource,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type Collection struct {
	ID         string            `json:"id"`
	Path       string            `json:"path"`
	Zone       string            `json:"zone"`
	ChildCount int               `json:"childCount"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}
