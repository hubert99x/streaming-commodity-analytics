# Security

The project applies several production-inspired hardening practices, but it is designed as a single-host educational system rather than a fully production-grade distributed deployment.

## Security Assumptions

This system assumes a trusted single-host environment:

- All services run on a single machine (Docker Compose).
- Internal Docker networks are not exposed externally.
- No untrusted users have access to the host or Docker daemon.
- External access is limited to localhost-bound services.

Under these assumptions, network-level encryption (TLS) and strong authentication between services are not enforced. If these assumptions are violated (e.g. multi-host deployment, exposed networks), additional controls such as TLS, service authentication, and secret management are required.

## Container Hardening

These controls reduce the attack surface and limit the impact of a compromised container.

| Control | Implementation |
|---------|---------------|
| No-new-privileges | `security_opt: no-new-privileges:true` on all containers |
| Capability drop | `cap_drop: ALL` on Spark, producer, alert-receiver, kafka-lag, retention, dbt-scheduler |
| Read-only rootfs | producer, alert-receiver, kafka-lag |
| Non-root users | Producer (appuser), alert-receiver (appuser), kafka-lag (appuser), Spark (uid 185), dbt-scheduler (uid 1000) |
| tmpfs /tmp | Producer, alert-receiver (no persistent writable disk) |
| Slim base images | python:3.11-slim, python:3.12-slim |
| Port binding | All external ports bound to `127.0.0.1` (localhost only) |

## Authentication & Access

| Control | Implementation |
|---------|---------------|
| Webhook auth | Alert-receiver requires `ALERT_WEBHOOK_TOKEN` (mandatory unless explicitly disabled) |
| Database RBAC | 5 distinct roles with least-privilege grants |
| Pre-commit hooks | gitleaks (secret detection in source code) |
| Code quality | ruff (Python linting) |
| CI supply chain | All GitHub Actions SHA-pinned to prevent tag-based attacks |

## Database Roles

The database access model follows the principle of least privilege — each service is granted only the minimum permissions required for its role.

| Role | Schemas | Permissions |
|------|---------|-------------|
| `spark_writer` | public, ingest, monitoring | INSERT raw_prices, CREATE staging tables, INSERT DLQ |
| `dbt_runner` | public, analytics, monitoring | SELECT raw_prices, CREATE analytics models, DELETE monitoring (retention) |
| `grafana_read` | analytics, monitoring | SELECT on all analytics tables (auto-granted on new dbt objects) + SELECT on all monitoring tables and views |
| `producer_writer` | monitoring | INSERT api_calls |
| `backup_user` | all | SELECT on all schemas + INSERT backup_log (used by pg_dump and backup logging) |

`DEFAULT PRIVILEGES FOR USER dbt_runner` auto-grants SELECT to `grafana_read` on any new table dbt creates. No role has superuser privileges; access is restricted to specific schemas and operations required by each service.

## CI Security Scanning

Trivy scans run automatically in CI on push/PR and weekly. Example local scan:
```bash
trivy image --scanners vuln --severity CRITICAL,HIGH --ignore-unfixed streaming_system-producer
```

CVE exceptions are tracked in `.trivyignore` with `Added:` and `Expires:` dates for quarterly review.

## Security Gaps

The following are intentionally not implemented due to the project scope:

- No TLS encryption between services (PostgreSQL, Kafka, HTTP).
- Default credentials in `.env.example` must be changed manually.
- No secret management (environment variables used directly).
- Backups are stored unencrypted on disk.
- No authentication for Kafka (PLAINTEXT).
- No database audit logging (e.g. statement logging, access tracing).

These gaps are acceptable within the assumed single-host environment but would be critical in a production deployment. For a detailed security analysis, see [TECHNICAL_DOCUMENTATION.md](TECHNICAL_DOCUMENTATION.md#7-security-model).
