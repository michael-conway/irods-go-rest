package domain

type User struct {
	ID    int64      `json:"id"`
	Name  string     `json:"name"`
	Zone  string     `json:"zone"`
	Type  string     `json:"type"`
	Links *UserLinks `json:"links,omitempty"`
}

type UserLinks struct {
	Self           *ActionLink `json:"self,omitempty"`
	Update         *ActionLink `json:"update,omitempty"`
	UpdateType     *ActionLink `json:"update_type,omitempty"`
	UpdatePassword *ActionLink `json:"update_password,omitempty"`
	Delete         *ActionLink `json:"delete,omitempty"`
	AVUs           *ActionLink `json:"avus,omitempty"`
	CreateAVU      *ActionLink `json:"create_avu,omitempty"`
}
