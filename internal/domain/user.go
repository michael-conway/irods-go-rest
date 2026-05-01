package domain

type User struct {
	ID    int64      `json:"id"`
	Name  string     `json:"name"`
	Zone  string     `json:"zone"`
	Type  string     `json:"type"`
	Links *UserLinks `json:"links,omitempty"`
}

type UserLinks struct {
	Self   *ActionLink `json:"self,omitempty"`
	Update *ActionLink `json:"update,omitempty"`
	Delete *ActionLink `json:"delete,omitempty"`
}
