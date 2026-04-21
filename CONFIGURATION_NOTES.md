# Configuration Notes

## Runtime configuration

`irods-go-rest` reads configuration from a YAML file plus `GOREST_*` environment variables.

The loader supports three configuration layers:

1. A YAML configuration file such as `rest-config.yaml`
2. Environment variable overrides with the `GOREST_` prefix
3. File-backed secrets for sensitive values

## Using a specific config file

If you want to point the service at one specific config file and skip all search paths, set:

```bash
IRODS_REST_CONFIG_FILE=/path/to/rest-config.yaml
```

When `IRODS_REST_CONFIG_FILE` is set, it overrides the default search locations and that exact file is used.

If `IRODS_REST_CONFIG_FILE` is not set, the loader searches for `rest-config.yaml` in its configured search paths and
then applies environment variable overrides on top of the file values.

## YAML configuration

The sample configuration file lives at:

```text
internal/config/rest-config.yaml
```

This file is used to configure:

- iRODS connection settings
- REST service settings such as `PublicURL` and `RestLogLevel`
- OIDC client settings for Keycloak integration

## Environment variable overrides

Environment variables override file values.

Examples:

```bash
GOREST_PUBLIC_URL=http://localhost:8080
GOREST_REST_LOG_LEVEL=debug
GOREST_IRODS_HOST=irods-provider
GOREST_IRODS_PORT=1247
GOREST_IRODS_ZONE=tempZone
GOREST_IRODS_ADMIN_USER=rods
GOREST_IRODS_DEFAULT_RESOURCE=demoResc
GOREST_OIDC_URL=http://keycloak:8080
GOREST_OIDC_INSECURE_SKIP_VERIFY=false
GOREST_OIDC_REALM=irods
GOREST_OIDC_CLIENT_ID=irods-go-rest
GOREST_OIDC_SCOPE="openid profile email"
```

Supported secret-bearing environment variables include:

```bash
GOREST_IRODS_ADMIN_PASSWORD=...
GOREST_OIDC_CLIENT_SECRET=...
```

but for production-style deployments, prefer the file-backed secret variants described below.

## File-backed secrets

The loader supports file-backed secrets for the sensitive values:

```yaml
IrodsAdminPasswordFile: /path/to/irods-admin-password.txt
OidcClientSecretFile: /path/to/oidc-client-secret.txt
```

and the matching environment variables:

```bash
GOREST_IRODS_ADMIN_PASSWORD_FILE=/path/to/irods-admin-password.txt
GOREST_OIDC_CLIENT_SECRET_FILE=/path/to/oidc-client-secret.txt
```

At startup the loader reads those files and trims trailing whitespace.

## Secret precedence

The effective precedence is:

1. Explicit secret value from environment or YAML
2. Secret file path from environment or YAML
3. Empty value if neither is provided

That means direct secret values still work, but mounted secret files are the preferred operational pattern.

## Recommended usage

For local development:

- keep non-secret settings in `rest-config.yaml`
- use `GOREST_*` environment variables when you need quick overrides

For containerized or production-style deployments:

- mount `rest-config.yaml` as a read-only config file
- mount secret files separately
- set `IRODS_REST_CONFIG_FILE` to the mounted config file path
- set `GOREST_IRODS_ADMIN_PASSWORD_FILE` and `GOREST_OIDC_CLIENT_SECRET_FILE` to the mounted secret paths

Example:

```bash
IRODS_REST_CONFIG_FILE=/config/rest-config.yaml
GOREST_IRODS_ADMIN_PASSWORD_FILE=/run/secrets/irods_admin_password
GOREST_OIDC_CLIENT_SECRET_FILE=/run/secrets/oidc_client_secret
```

This keeps non-secret configuration and secrets separate and makes the deployment model consistent with `irods-go-drs`.

## Self-signed local Keycloak certificates

If your local Keycloak uses a self-signed certificate, you can disable OIDC TLS certificate verification for development only:

```yaml
OidcUrl: https://localhost:8443
OidcInsecureSkipVerify: true
```

or:

```bash
GOREST_OIDC_URL=https://localhost:8443
GOREST_OIDC_INSECURE_SKIP_VERIFY=true
```

This should only be used on a developer machine. It should not be enabled in production.
