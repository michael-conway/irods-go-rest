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
GOREST_REST_LOG_LEVEL=info

GOREST_IRODS_HOST=irods-provider
GOREST_IRODS_PORT=1247
GOREST_IRODS_ZONE=tempZone
GOREST_IRODS_ADMIN_USER=rods
GOREST_IRODS_DEFAULT_RESOURCE=demoResc

GOREST_OIDC_URL=https://localhost:8443
GOREST_OIDC_REALM=drs
GOREST_OIDC_CLIENT_ID=irods-go-rest
GOREST_OIDC_SCOPE="openid profile email"
GOREST_OIDC_INSECURE_SKIP_VERIFY=false
```

If your local Keycloak uses a self-signed certificate, you can temporarily use:

```bash
GOREST_OIDC_INSECURE_SKIP_VERIFY=true
```

Use that only for local development.

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
