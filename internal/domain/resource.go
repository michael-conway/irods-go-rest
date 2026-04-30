package domain

import "time"

type Resource struct {
	ID        int64          `json:"id"`
	Name      string         `json:"name"`
	Zone      string         `json:"zone,omitempty"`
	Type      string         `json:"type,omitempty"`
	Class     string         `json:"class,omitempty"`
	Location  string         `json:"location,omitempty"`
	Path      string         `json:"path,omitempty"`
	Context   string         `json:"context,omitempty"`
	CreatedAt *time.Time     `json:"created_at,omitempty"`
	UpdatedAt *time.Time     `json:"updated_at,omitempty"`
	Links     *ResourceLinks `json:"links,omitempty"`
}

type ResourceLinks struct {
	Self *ActionLink `json:"self,omitempty"`
}
