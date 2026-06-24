package domain

type UserGroupSummary struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Zone        string `json:"zone"`
	Type        string `json:"type"`
	MemberCount int    `json:"member_count"`
}

type UserMembershipSummary struct {
	ID     int64          `json:"id"`
	Name   string         `json:"name"`
	Zone   string         `json:"zone"`
	Type   string         `json:"type"`
	Groups []UserGroupRef `json:"groups"`
}

type CurrentUserMembership struct {
	User                        User           `json:"user"`
	Groups                      []UserGroupRef `json:"groups"`
	IsRodsAdmin                 bool           `json:"is_rodsadmin"`
	IsGroupAdmin                bool           `json:"is_groupadmin"`
	CanAdministerUsersAndGroups bool           `json:"can_administer_users_and_groups"`
}

type UserGroupRef struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	Zone string `json:"zone"`
	Type string `json:"type"`
}

type PrincipalSearchResult struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Zone  string `json:"zone"`
	Type  string `json:"type"`
	Kind  string `json:"kind"`
	Score int    `json:"score"`
}
