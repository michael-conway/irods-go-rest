package domain

type Favorite struct {
	Name         string         `json:"name"`
	AbsolutePath string         `json:"absolute_path"`
	Links        *FavoriteLinks `json:"links,omitempty"`
}

type FavoriteLinks struct {
	Self    *ActionLink `json:"self,omitempty"`
	Details *ActionLink `json:"details,omitempty"`
	Update  *ActionLink `json:"update,omitempty"`
	Delete  *ActionLink `json:"delete,omitempty"`
}

type FavoriteCollectionLinks struct {
	Self   *ActionLink `json:"self,omitempty"`
	Create *ActionLink `json:"create,omitempty"`
	Update *ActionLink `json:"update,omitempty"`
	Delete *ActionLink `json:"delete,omitempty"`
}
