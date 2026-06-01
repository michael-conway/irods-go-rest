# Configuration Notes

This is the runtime configuration reference for `irods-go-rest`.

## Sources And Precedence

The service reads `rest-config.yaml` by default. To use one exact file, set:

```bash
IRODS_REST_CONFIG_FILE=/path/to/rest-config.yaml
```

Search paths without `IRODS_REST_CONFIG_FILE`:

1. paths passed by the caller
2. `/etc/irods-ext/`
3. `$HOME/.irods-go-rest`
4. current working directory

Environment variables override config-file values. Secret-file settings are used only when the corresponding explicit secret value is empty.

## Common Runtime Settings

```bash
GOREST_PUBLIC_URL=http://localhost:8080
IRODS_REST_ADDR=:8080
GOREST_WEB_ENABLED=false
GOREST_TRUST_FORWARDED_HEADERS=false
GOREST_CORS_ALLOWED_ORIGINS=http://localhost:8081,http://localhost:5173
GOREST_REST_LOG_LEVEL=info
```

`GOREST_PUBLIC_URL` is the externally reachable service URL used for redirects and generated links. `IRODS_REST_ADDR` is the HTTP bind address; use `:8080` in containers.

Keep `GOREST_TRUST_FORWARDED_HEADERS=false` unless REST is behind a trusted proxy that controls `X-Forwarded-*` headers. In untrusted environments, prefer explicit `GOREST_PUBLIC_URL`.

HTTP hardening defaults are applied when unset or non-positive:

```bash
GOREST_HTTP_READ_TIMEOUT_SECONDS=30
GOREST_HTTP_READ_HEADER_TIMEOUT_SECONDS=5
GOREST_HTTP_WRITE_TIMEOUT_SECONDS=30
GOREST_HTTP_IDLE_TIMEOUT_SECONDS=120
GOREST_HTTP_MAX_HEADER_BYTES=1048576
```

## iRODS Connection

```bash
GOREST_IRODS_HOST=irods-provider
GOREST_IRODS_PORT=1247
GOREST_IRODS_ZONE=tempZone
GOREST_IRODS_ADMIN_USER=rods
GOREST_IRODS_ADMIN_PASSWORD=rods
GOREST_IRODS_ADMIN_LOGIN_TYPE=native
GOREST_IRODS_AUTH_SCHEME=native
GOREST_IRODS_DEFAULT_RESOURCE=providerResc
GOREST_IRODS_NEGOTIATION_POLICY=CS_NEG_DONT_CARE
```

`IrodsAdminLoginType` controls the admin/proxy account used by bearer-token and ticket-backed requests. `IrodsAuthScheme` controls direct user credentials, including Basic auth requests.

PAM auth requires SSL in go-irodsclient. If the iRODS server returns `CS_NEG_REFUSE`, use native auth for that connection path or enable SSL negotiation on the iRODS server.

## iRODS SSL

For SSL-configured zones:

```yaml
IrodsNegotiationPolicy: CS_NEG_REQUIRE
IrodsSSLConfig:
  CACertificateFile: /etc/irods/ca.pem
  CACertificatePath:
  EncryptionKeySize: 32
  EncryptionAlgorithm: AES-256-CBC
  EncryptionSaltSize: 8
  EncryptionNumHashRounds: 16
  VerifyServer: hostname
  DHParamsFile:
  ServerName: irods.example.org
```

Environment equivalents:

```bash
GOREST_IRODS_NEGOTIATION_POLICY=CS_NEG_REQUIRE
GOREST_IRODS_SSL_CA_CERTIFICATE_FILE=/etc/irods/ca.pem
GOREST_IRODS_SSL_CA_CERTIFICATE_PATH=
GOREST_IRODS_ENCRYPTION_KEY_SIZE=32
GOREST_IRODS_ENCRYPTION_ALGORITHM=AES-256-CBC
GOREST_IRODS_ENCRYPTION_SALT_SIZE=8
GOREST_IRODS_ENCRYPTION_NUM_HASH_ROUNDS=16
GOREST_IRODS_SSL_VERIFY_SERVER=hostname
GOREST_IRODS_SSL_DH_PARAMS_FILE=
GOREST_IRODS_SSL_SERVER_NAME=irods.example.org
```

`VerifyServer` accepts `hostname`, `cert`, or `none`. Empty encryption settings fall back to go-irodsclient defaults.

## OIDC And Browser Login

```bash
GOREST_OIDC_URL=https://keycloak:8443
GOREST_OIDC_AUTH_URL=https://localhost:8443
GOREST_OIDC_REALM=drs
GOREST_OIDC_CLIENT_ID=irods-go-rest
GOREST_OIDC_CLIENT_SECRET=secret
GOREST_OIDC_SCOPE="openid profile email"
GOREST_OIDC_INSECURE_SKIP_VERIFY=false
```

`GOREST_OIDC_URL` is used by REST for backend token exchange and validation. `GOREST_OIDC_AUTH_URL` controls the browser redirect target for `/web/login`; if unset, REST uses `GOREST_OIDC_URL`.

`GOREST_WEB_ENABLED` controls `/web/*` routes. Keep it disabled for deployments that do not need REST-hosted browser login. When enabled, web sessions are in-memory, bounded, and expiring:

- session expiry follows token `expires_in` when present
- fallback session TTL is `15m`
- maximum sessions: `1024`, with oldest-session eviction

For self-signed local Keycloak certificates, use `GOREST_OIDC_INSECURE_SKIP_VERIFY=true` only in development.

## Resource And Replica Settings

```bash
GOREST_RESOURCE_AFFINITY=providerResc,resourceResc
GOREST_REPLICA_TRIM_MIN_COPIES=1
GOREST_REPLICA_TRIM_MIN_AGE_MINUTES=0
```

`ResourceAffinity` is optional and advertises iRODS resources proximate to this REST instance. YAML form:

```yaml
ResourceAffinity:
  - providerResc
  - resourceResc
```

Replica trim defaults are used by `PATCH /api/v1/path/replicas` and `DELETE /api/v1/path/replicas` when the request body does not supply `min_copies` or `min_age_minutes`.

## S3 Admin Extension

```bash
GOREST_S3_API_SUPPORTED=true
GOREST_S3_BUCKET_MAPPING_FILE=/shared-s3-config/irods-s3-bucket-mapping.json
GOREST_S3_USER_MAPPING_FILE=/shared-s3-config/irods-s3-user-mapping.json
```

`S3ApiSupported=false` makes `/api/v1/ext/s3/*` return `501 not_supported`.

`S3BucketMappingFile` must be an absolute path to the iRODS S3 API bucket mapping JSON. REST rewrites it after successful bucket AVU mutations and refresh operations.

`S3UserMappingFile` must be an absolute path to the iRODS S3 API user mapping JSON. REST rewrites it after S3 user secret updates and user mapping refresh operations.

S3 user-secret authorization:

- listing all secrets and refreshing the user mapping require rodsadmin
- per-user get/create/update/delete is self-only by default
- rodsadmin may operate on any user

## Secrets

Prefer secret files over inline secrets in container deployments:

```yaml
IrodsAdminPasswordFile: /run/secrets/irods_admin_password
OidcClientSecretFile: /run/secrets/oidc_client_secret
```

Environment equivalents:

```bash
GOREST_IRODS_ADMIN_PASSWORD_FILE=/run/secrets/irods_admin_password
GOREST_OIDC_CLIENT_SECRET_FILE=/run/secrets/oidc_client_secret
```

Secret resolution order:

1. explicit value, such as `IrodsAdminPassword` or `GOREST_IRODS_ADMIN_PASSWORD`
2. secret file
3. empty value

## Test Settings

Integration and E2E tests share `GOREST_E2E_CONFIG_FILE`.

Useful test-only keys:

```yaml
IrodsPrimaryTestUser: test1
IrodsPrimaryTestPassword: test
IrodsSecondaryTestUser: test2
IrodsSecondaryTestPassword: test
TestResource1: providerResc
TestResource2: resourceResc
TestBearerToken: ""
```

Use [e2e/rest-config.e2e.sample.yaml](./e2e/rest-config.e2e.sample.yaml) with `irods-grid-stack` as the starting point for host-run live tests.

## Container Pattern

Mount config and secrets separately:

```bash
IRODS_REST_CONFIG_FILE=/config/rest-config.yaml
GOREST_IRODS_ADMIN_PASSWORD_FILE=/run/secrets/irods_admin_password
GOREST_OIDC_CLIENT_SECRET_FILE=/run/secrets/oidc_client_secret
```

For `irods-grid-stack`, prefer setting REST environment through that stack's `.env` and mounted config files rather than editing this repository's sample config for deployment-specific values.
