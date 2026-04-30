package domain

type UserGroup struct {
	ID      int64             `json:"id"`
	Name    string            `json:"name"`
	Zone    string            `json:"zone"`
	Type    string            `json:"type"`
	Members []UserGroupMember `json:"members,omitempty"`
	Links   *UserGroupLinks   `json:"links,omitempty"`
}

type UserGroupMember struct {
	ID    int64                 `json:"id"`
	Name  string                `json:"name"`
	Zone  string                `json:"zone"`
	Type  string                `json:"type"`
	Links *UserGroupMemberLinks `json:"links,omitempty"`
}

type UserGroupLinks struct {
	Self      *ActionLink `json:"self,omitempty"`
	Delete    *ActionLink `json:"delete,omitempty"`
	AddMember *ActionLink `json:"add_member,omitempty"`
}

type UserGroupMemberLinks struct {
	Self            *ActionLink `json:"self,omitempty"`
	RemoveFromGroup *ActionLink `json:"remove_from_group,omitempty"`
}
