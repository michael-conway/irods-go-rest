# Configuration Notes

Use this file as the quick reference for `irods-go-rest` configuration.

## Config sources

The service reads configuration in this order:

1. `rest-config.yaml`
2. `GOREST_*` environment variable overrides
3. Secret files for sensitive values

To use one exact config file, set:

```bash
IRODS_REST_CONFIG_FILE=/path/to/rest-config.yaml
```

## Main runtime settings

These are the settings you will usually care about:

```bash
GOREST_PUBLIC_URL=http://localhost:8080
IRODS_REST_ADDR=:8080
GOREST_REST_LOG_LEVEL=info

GOREST_IRODS_HOST=irods-provider
GOREST_IRODS_PORT=1247
GOREST_IRODS_ZONE=tempZone
GOREST_IRODS_ADMIN_USER=rods
GOREST_IRODS_ADMIN_LOGIN_TYPE=native
GOREST_IRODS_AUTH_SCHEME=native
GOREST_IRODS_DEFAULT_RESOURCE=demoResc
GOREST_IRODS_NEGOTIATION_POLICY=CS_NEG_DONT_CARE
GOREST_RESOURCE_AFFINITY=demoResc,edgeResc
GOREST_S3_BUCKET_MAPPING_FILE=/config/irods-s3-bucket-mapping.json
GOREST_REPLICA_TRIM_MIN_COPIES=1
GOREST_REPLICA_TRIM_MIN_AGE_MINUTES=0

GOREST_OIDC_URL=https://localhost:8443
GOREST_OIDC_REALM=drs
GOREST_OIDC_CLIENT_ID=irods-go-rest
GOREST_OIDC_SCOPE="openid profile email"
GOREST_OIDC_INSECURE_SKIP_VERIFY=false
```

`PublicURL` is the externally reachable URL used for redirects and generated
links. `IRODS_REST_ADDR` is the socket address the HTTP server binds to. In
containers, use `IRODS_REST_ADDR=:8080` so Docker port publishing can reach the
service.

If your local Keycloak uses a self-signed certificate, you can temporarily use:

```bash
GOREST_OIDC_INSECURE_SKIP_VERIFY=true
```

Use that only for local development.

## iRODS SSL

For SSL-configured iRODS servers, set the negotiation policy to require SSL and
provide the optional SSL settings needed by that zone:

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

Environment variable equivalents:

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

`VerifyServer` accepts `hostname`, `cert`, or `none`. Empty encryption settings
default to the go-irodsclient defaults.

`IrodsAdminLoginType` controls the admin/proxy account used by bearer-token and
ticket-backed requests. `IrodsAuthScheme` controls direct user credentials, such
as Basic auth requests. PAM auth requires SSL in go-irodsclient; if the iRODS
server returns `CS_NEG_REFUSE`, use native auth for that connection path or
enable SSL negotiation on the iRODS server.

## Resource affinity

`ResourceAffinity` is optional and represents iRODS resources that are
considered proximate to this service instance.

`S3BucketMappingFile` enables `/api/v1/ext/s3/*` administration routes and must
be the absolute path to the iRODS S3 API local-file bucket mapping JSON. The REST
service rewrites this file after successful bucket AVU mutations so the S3 API
can reload the mapping.

Supported forms:

```yaml
ResourceAffinity:
  - demoResc
  - edgeResc
```

or environment override:

```bash
GOREST_RESOURCE_AFFINITY=demoResc,edgeResc
```

## Replica trim defaults

`ReplicaTrimMinCopies` and `ReplicaTrimMinAgeMinutes` are optional runtime defaults
used by `PATCH /api/v1/path/replicas` and `DELETE /api/v1/path/replicas` when
`min_copies` / `min_age_minutes` are not supplied in the request body.

```yaml
ReplicaTrimMinCopies: 1
ReplicaTrimMinAgeMinutes: 0
```

or environment override:

```bash
GOREST_REPLICA_TRIM_MIN_COPIES=1
GOREST_REPLICA_TRIM_MIN_AGE_MINUTES=0
```

## Secrets

Prefer secret files over inline secrets.

Supported file-backed secret settings:

```yaml
IrodsAdminPasswordFile: /run/secrets/irods_admin_password
OidcClientSecretFile: /run/secrets/oidc_client_secret
```

Environment variable equivalents:

```bash
GOREST_IRODS_ADMIN_PASSWORD_FILE=/run/secrets/irods_admin_password
GOREST_OIDC_CLIENT_SECRET_FILE=/run/secrets/oidc_client_secret
```

Secret precedence is:

1. explicit value
2. secret file
3. empty

## Recommended pattern

For local development:

- keep normal settings in `rest-config.yaml`
- use `GOREST_*` for quick overrides

For containers:

- mount the config file
- mount secrets separately
- point `IRODS_REST_CONFIG_FILE` at the mounted config file

Example:

```bash
IRODS_REST_CONFIG_FILE=/config/rest-config.yaml
GOREST_IRODS_ADMIN_PASSWORD_FILE=/run/secrets/irods_admin_password
GOREST_OIDC_CLIENT_SECRET_FILE=/run/secrets/oidc_client_secret
```
