package httpapi

import (
	"net/http"
	"strings"

	api "github.com/michael-conway/irods-go-rest/api"
	"github.com/michael-conway/irods-go-rest/internal/auth"
	"github.com/michael-conway/irods-go-rest/internal/config"
	"github.com/michael-conway/irods-go-rest/internal/restservice"
)

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>iRODS REST API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({
      url: "/openapi.yaml",
      dom_id: "#swagger-ui"
    });
  </script>
</body>
</html>
`

type Handler struct {
	cfg        config.RestConfig
	paths      restservice.PathService
	s3Admin    restservice.S3AdminService
	serverInfo restservice.ServerInfoService
	resources  restservice.ResourceService
	users      restservice.UserService
	userGroups restservice.UserGroupService
	tickets    restservice.TicketService
	authFlow   auth.AuthFlowService
	verifier   auth.TokenVerifier
	webSession *auth.SessionStore
}

func NewHandler(cfg config.RestConfig, paths restservice.PathService, s3Admin restservice.S3AdminService, serverInfo restservice.ServerInfoService, resources restservice.ResourceService, users restservice.UserService, userGroups restservice.UserGroupService, tickets restservice.TicketService, authFlow auth.AuthFlowService, verifier auth.TokenVerifier, webSession *auth.SessionStore) *Handler {
	return &Handler{
		cfg:        cfg,
		paths:      paths,
		s3Admin:    s3Admin,
		serverInfo: serverInfo,
		resources:  resources,
		users:      users,
		userGroups: userGroups,
		tickets:    tickets,
		authFlow:   authFlow,
		verifier:   verifier,
		webSession: webSession,
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.getHealth)
	mux.HandleFunc("GET /openapi.yaml", h.getOpenAPISpec)
	mux.HandleFunc("GET /swagger", h.getSwaggerUI)
	mux.HandleFunc("GET /web/", h.webHome)
	mux.HandleFunc("GET /web/login", h.webLogin)
	mux.HandleFunc("GET /web/callback", h.webCallback)
	mux.HandleFunc("POST /web/logout", h.webLogout)
	mux.Handle("GET /api/v1/path", h.requireBearer(http.HandlerFunc(h.getPath)))
	mux.Handle("POST /api/v1/path", h.requireBearer(http.HandlerFunc(h.postPath)))
	mux.Handle("PATCH /api/v1/path", h.requireBearer(http.HandlerFunc(h.patchPath)))
	mux.Handle("DELETE /api/v1/path", h.requireBearer(http.HandlerFunc(h.deletePath)))
	mux.Handle("GET /api/v1/path/children", h.requireBearer(http.HandlerFunc(h.getPathChildren)))
	mux.Handle("GET /api/v1/path/replicas", h.requireBearer(http.HandlerFunc(h.getPathReplicas)))
	mux.Handle("POST /api/v1/path/replicas", h.requireBearer(http.HandlerFunc(h.postPathReplicas)))
	mux.Handle("PATCH /api/v1/path/replicas", h.requireBearer(http.HandlerFunc(h.patchPathReplicas)))
	mux.Handle("DELETE /api/v1/path/replicas", h.requireBearer(http.HandlerFunc(h.deletePathReplicas)))
	mux.Handle("GET /api/v1/path/acl", h.requireBearer(http.HandlerFunc(h.getPathACL)))
	mux.Handle("POST /api/v1/path/acl", h.requireBearer(http.HandlerFunc(h.postPathACL)))
	mux.Handle("PUT /api/v1/path/acl/{acl_id}", h.requireBearer(http.HandlerFunc(h.putPathACL)))
	mux.Handle("DELETE /api/v1/path/acl/{acl_id}", h.requireBearer(http.HandlerFunc(h.deletePathACL)))
	mux.Handle("PUT /api/v1/path/acl/inheritance", h.requireBearer(http.HandlerFunc(h.putPathACLInheritance)))
	mux.Handle("DELETE /api/v1/path/acl/inheritance", h.requireBearer(http.HandlerFunc(h.deletePathACLInheritance)))
	mux.Handle("GET /api/v1/path/avu", h.requireBearer(http.HandlerFunc(h.getPathAVUs)))
	mux.Handle("POST /api/v1/path/avu", h.requireBearer(http.HandlerFunc(h.postPathAVU)))
	mux.Handle("PUT /api/v1/path/avu/{avu_id}", h.requireBearer(http.HandlerFunc(h.putPathAVU)))
	mux.Handle("DELETE /api/v1/path/avu/{avu_id}", h.requireBearer(http.HandlerFunc(h.deletePathAVU)))
	mux.Handle("GET /api/v1/path/checksum", h.requireBearer(http.HandlerFunc(h.getPathChecksum)))
	mux.Handle("POST /api/v1/path/checksum", h.requireBearer(http.HandlerFunc(h.postPathChecksum)))
	mux.Handle("POST /api/v1/path/ticket", h.requireBearer(http.HandlerFunc(h.postPathTicket)))
	mux.Handle("GET /api/v1/server", h.requireBearer(http.HandlerFunc(h.getServerInfo)))
	mux.Handle("GET /api/v1/resource", h.requireBearer(http.HandlerFunc(h.getResources)))
	mux.Handle("GET /api/v1/resource/{resource_id}", h.requireBearer(http.HandlerFunc(h.getResource)))
	mux.Handle("GET /api/v1/user", h.requireBearer(http.HandlerFunc(h.getUsers)))
	mux.Handle("POST /api/v1/user", h.requireBearer(http.HandlerFunc(h.postUser)))
	mux.Handle("GET /api/v1/user/{user_name}", h.requireBearer(http.HandlerFunc(h.getUser)))
	mux.Handle("PUT /api/v1/user/{user_name}", h.requireBearer(http.HandlerFunc(h.putUser)))
	mux.Handle("DELETE /api/v1/user/{user_name}", h.requireBearer(http.HandlerFunc(h.deleteUser)))
	mux.Handle("GET /api/v1/usergroup", h.requireBearer(http.HandlerFunc(h.getUserGroups)))
	mux.Handle("POST /api/v1/usergroup", h.requireBearer(http.HandlerFunc(h.postUserGroup)))
	mux.Handle("GET /api/v1/usergroup/{group_name}", h.requireBearer(http.HandlerFunc(h.getUserGroup)))
	mux.Handle("DELETE /api/v1/usergroup/{group_name}", h.requireBearer(http.HandlerFunc(h.deleteUserGroup)))
	mux.Handle("POST /api/v1/usergroup/{group_name}/member", h.requireBearer(http.HandlerFunc(h.postUserGroupMember)))
	mux.Handle("DELETE /api/v1/usergroup/{group_name}/member/{user_name}", h.requireBearer(http.HandlerFunc(h.deleteUserGroupMember)))
	mux.Handle("HEAD /api/v1/path/contents", h.requireDownloadBearer(http.HandlerFunc(h.headPathContents)))
	mux.Handle("GET /api/v1/path/contents", h.requireDownloadBearer(http.HandlerFunc(h.getPathContents)))
	mux.Handle("POST /api/v1/path/contents", h.requireBearer(http.HandlerFunc(h.postPathContents)))
	mux.Handle("GET /api/v1/ticket", h.requireBearer(http.HandlerFunc(h.getTickets)))
	mux.Handle("POST /api/v1/ticket", h.requireBearer(http.HandlerFunc(h.postTicket)))
	mux.Handle("GET /api/v1/ticket/{ticket_name}", h.requireBearer(http.HandlerFunc(h.getTicket)))
	mux.Handle("PATCH /api/v1/ticket/{ticket_name}", h.requireBearer(http.HandlerFunc(h.patchTicket)))
	mux.Handle("DELETE /api/v1/ticket/{ticket_name}", h.requireBearer(http.HandlerFunc(h.deleteTicket)))
	mux.Handle("GET /api/v1/ext/favorites", h.requireBearer(http.HandlerFunc(h.getExtFavorites)))
	mux.Handle("POST /api/v1/ext/favorites", h.requireBearer(http.HandlerFunc(h.postExtFavorite)))
	mux.Handle("PUT /api/v1/ext/favorites", h.requireBearer(http.HandlerFunc(h.putExtFavorite)))
	mux.Handle("DELETE /api/v1/ext/favorites", h.requireBearer(http.HandlerFunc(h.deleteExtFavorite)))
	mux.Handle("GET /api/v1/ext/s3/buckets", h.requireBearer(http.HandlerFunc(h.getExtS3Buckets)))
	mux.Handle("POST /api/v1/ext/s3/buckets", h.requireBearer(http.HandlerFunc(h.postExtS3Bucket)))
	mux.Handle("PUT /api/v1/ext/s3/buckets", h.requireBearer(http.HandlerFunc(h.putExtS3Bucket)))
	// TODO: Temporary S3 mapping reconciliation endpoint. Remove after bucket mapping is managed by the dedicated S3 admin service flow.
	mux.Handle("POST /api/v1/ext/s3/buckets/refresh-mapping", h.requireBearer(http.HandlerFunc(h.postExtS3BucketMappingRefresh)))
	mux.Handle("GET /api/v1/ext/s3/buckets/by-path", h.requireBearer(http.HandlerFunc(h.getExtS3BucketByPath)))
	mux.Handle("GET /api/v1/ext/s3/buckets/{bucket_id}", h.requireBearer(http.HandlerFunc(h.getExtS3Bucket)))
	mux.Handle("DELETE /api/v1/ext/s3/buckets/{bucket_id}", h.requireBearer(http.HandlerFunc(h.deleteExtS3Bucket)))

	return requestLogger(mux)
}

func pathValue(r *http.Request, key string) string {
	return strings.TrimSpace(r.PathValue(key))
}

func (h *Handler) getOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(api.OpenAPISpec)
}

func (h *Handler) getSwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUIHTML))
}
