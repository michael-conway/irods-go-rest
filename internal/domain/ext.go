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

type S3Bucket struct {
	BucketID  string         `json:"bucket_id"`
	IRODSPath string         `json:"irods_path"`
	Links     *S3BucketLinks `json:"links,omitempty"`
}

type S3BucketLinks struct {
	Self    *ActionLink `json:"self,omitempty"`
	Path    *ActionLink `json:"path,omitempty"`
	Details *ActionLink `json:"details,omitempty"`
	Update  *ActionLink `json:"update,omitempty"`
	Delete  *ActionLink `json:"delete,omitempty"`
}

type S3BucketCollectionLinks struct {
	Self   *ActionLink `json:"self,omitempty"`
	Create *ActionLink `json:"create,omitempty"`
	Update *ActionLink `json:"update,omitempty"`
}

type S3BucketMappingRefresh struct {
	MappingFilePath string     `json:"mapping_file_path,omitempty"`
	Buckets         []S3Bucket `json:"buckets"`
	Count           int        `json:"count"`
}

type S3UserSecret struct {
	UserName     string             `json:"user_name"`
	UserHomePath string             `json:"user_home_path,omitempty"`
	IRODSPath    string             `json:"irods_path,omitempty"`
	SecretKey    string             `json:"secret_key,omitempty"`
	Links        *S3UserSecretLinks `json:"links,omitempty"`
}

type S3UserSecretLinks struct {
	Self   *ActionLink `json:"self,omitempty"`
	Update *ActionLink `json:"update,omitempty"`
	Delete *ActionLink `json:"delete,omitempty"`
}

type S3UserSecretMappingRefresh struct {
	MappingFilePath string         `json:"mapping_file_path,omitempty"`
	Users           []S3UserSecret `json:"users"`
	Count           int            `json:"count"`
}
