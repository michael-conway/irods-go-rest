package domain

type ServerInfo struct {
	ReleaseVersion       string   `json:"release_version,omitempty"`
	APIVersion           string   `json:"api_version,omitempty"`
	ReconnectPort        int      `json:"reconnect_port,omitempty"`
	ReconnectAddr        string   `json:"reconnect_addr,omitempty"`
	Cookie               int      `json:"cookie,omitempty"`
	IRODSHost            string   `json:"irods_host,omitempty"`
	IRODSPort            int      `json:"irods_port,omitempty"`
	IRODSZone            string   `json:"irods_zone,omitempty"`
	IRODSNegotiation     string   `json:"irods_negotiation,omitempty"`
	IRODSDefaultResource string   `json:"irods_default_resource,omitempty"`
	ResourceAffinity     []string `json:"resource_affinity,omitempty"`
}
