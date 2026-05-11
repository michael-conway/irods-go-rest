# Alpha Release Checklist

This checklist defines the minimum readiness criteria for an alpha release of `irods-go-rest`.

## P0 - Must Complete Before Alpha

- [x] API contract and implementation alignment.
  - Verify every path/operation in `api/openapi.yaml` has matching runtime behavior (status codes, auth requirements, response shape), including `/api/v1/ext/*`.
  - Acceptance: no undocumented routes and no documented routes that are missing or behaviorally inconsistent.
  - Status (2026-05-10): removed stale `ext/filecarts` contract entries, added `GET /api/v1/ext/metadata-manifest` to OpenAPI, and added `internal/httpapi/openapi_contract_test.go` to enforce route/contract parity for `/healthz` and `/api/v1/*`.

- [x] Response semantics normalization.
  - Ensure all handlers use consistent error envelope and status policy for auth, validation, not-found, forbidden, unsupported, and internal failures.
  - Acceptance: clients can rely on predictable status + error body patterns across endpoint families.

- [x] AuthN/AuthZ hardening for mutating operations.
  - Confirm authorization gates for user/group/ticket/admin-affecting operations are explicit and tested.
  - Acceptance: non-admin users cannot perform admin-only actions; failures are explicit (`401` vs `403`).
  - Status (2026-05-10): user/group mutating operations enforce explicit role checks in `internal/irods/user.go` and `internal/irods/usergroup.go`; ticket mutations enforce owner scoping in `internal/irods/ticket.go`. Added HTTP tests for unauthenticated `401` on mutating user/group/ticket routes and non-admin `403` on group-admin mutations.

- [x] Content streaming and range correctness.
  - Validate `GET/HEAD /api/v1/path/contents` behavior for full download, valid ranges, invalid ranges, ticket-based auth, and large-file streaming.
  - Acceptance: range semantics and headers are RFC-consistent and stable under load.
  - Status (2026-05-10): added coverage for ticket/bearer/basic download auth, valid prefix and suffix ranges, invalid/unsatisfiable ranges (`416` + `Content-Range: bytes */N`), HEAD header-only semantics, and large-object streaming (1 MiB fixture). Updated range parsing to support suffix byte ranges and ignore unknown range units, and to ignore `Range` for `HEAD` per RFC 7233 §3.1.

- [x] Dependency and reproducible build hygiene.
  - Ensure pinned module versions resolve remotely; no local `replace` assumptions.
  - Enforce `GOWORK=off` in CI for build/test jobs.
  - Acceptance: CI passes from a clean checkout with workspace disabled.
  - Status (2026-05-11): no `replace` directives in `go.mod`; CI Go workflow now runs `go mod download`, `go mod verify`, `go build`, and `go test` with `GOWORK=off`; Docker build stage and CodeQL job also set `GOWORK=off`.

- [x] Documentation correctness pass.
  - Remove stale placeholders and keep docs aligned with runtime behavior.
  - Acceptance: README/OpenAPI/developer notes are internally consistent.
  - Status (2026-05-11): README placeholder metadata removed (`Current Version=0.1.0`, `License=BSD-2-Clause`); stale FileCart schemas removed from OpenAPI to match current runtime extension surface.

## P1 - Strongly Recommended Before Alpha

- [x] Observability baseline.
  - Move request logging to structured logs with route, method, status, duration, auth mode, principal (when available), path/operation identifiers, and error class.
  - Acceptance: production debugging can trace failures without ad hoc code changes.
  - Status (2026-05-11): request middleware now emits structured `slog` records with `route`, `method`, `status`, `duration_ms`, `auth_mode`, `principal`, request identifiers (for example `irods_path` and path IDs), `error_code`, and `error_class`. Auth middleware stamps auth metadata for both success and failure paths. Added request-logger unit tests for success, authorization failure, and error classification.

- [x] E2E/integration coverage expansion for critical workflows.
  - Cover path CRUD, AVU CRUD, ACL/inheritance, replicas, checksum, tickets, user/group admin flows, and key extension endpoints.
  - Acceptance: happy-path and key negative-path coverage for high-risk endpoints.

- [x] Config-source consistency for test harnesses.
  - Decide whether `GOREST_E2E_CONFIG_FILE` is the sole source or env-overrides remain supported; document and enforce consistently across `e2e/` and `internal/irods` integration tests.
  - Acceptance: one clear configuration contract with no conflicting fallback behavior.

- [ ] Extension endpoint support matrix.
  - Document what is fully supported vs intentionally unavailable (especially S3 admin-related operations) and ensure status behavior is explicit.
  - Acceptance: clients can distinguish stable alpha endpoints from non-supported extension operations.

## P2 - Post-Alpha Hardening

- [ ] Performance and backpressure profiling.
  - Characterize memory/latency for large transfers and high-concurrency metadata/list operations.

- [ ] Security/operations maturity.
  - Tighten browser-auth session controls, secrets handling guidance, and incident/debug playbooks.

- [ ] Compatibility matrix.
  - Publish tested iRODS versions, auth modes, and known feature limits.

## Exit Criteria for Alpha

- [ ] All P0 items complete.
- [ ] No known critical defects in auth, path resolution, or content transfer workflows.
- [ ] CI green with workspace disabled and reproducible module resolution.
- [ ] Primary docs accurately describe shipped behavior and unsupported features.
