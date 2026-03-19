# Security

The project applies several production-inspired hardening practices, but it is designed as a single-host educational and portfolio system rather than a fully production-grade distributed deployment.

## Container Hardening

| Control | Implementation |
|---------|---------------|
| No-new-privileges | `security_opt: no-new-privileges:true` on all containers |
| Capability drop | `cap_drop: ALL` on Spark, producer, alert-receiver, kafka-lag, retention, dbt-scheduler |
| Read-only rootfs | Spark, producer, alert-receiver, kafka-lag |
| Non-root users | Producer (appuser), alert-receiver (appuser), kafka-lag (appuser), Spark (uid 185), dbt-scheduler (uid 1000) |
| tmpfs /tmp | Producer, alert-receiver (no persistent writable disk) |
| Slim base images | python:3.11-slim, python:3.12-slim |
| Port binding | All external ports bound to `127.0.0.1` (localhost only) |

## Authentication & Access

| Control | Implementation |
|---------|---------------|
| Webhook auth | Alert-receiver requires `ALERT_WEBHOOK_TOKEN` (mandatory unless explicitly disabled) |
| Database RBAC | 5 distinct roles with least-privilege grants |
| Pre-commit hooks | gitleaks (secret detection) + ruff (Python linting) |
| CI supply chain | All GitHub Actions SHA-pinned to prevent tag-based attacks |

## Database Roles

| Role | Schemas | Permissions |
|------|---------|-------------|
| `spark_writer` | public, ingest, monitoring | INSERT raw_prices, CREATE staging tables, INSERT DLQ |
| `dbt_runner` | public, analytics | SELECT raw_prices, CREATE analytics models, DELETE monitoring (retention) |
| `grafana_read` | analytics, monitoring | SELECT only (auto-granted on new dbt objects) |
| `producer_writer` | monitoring | INSERT api_calls |
| `backup_user` | all | Superuser for pg_dump |

`DEFAULT PRIVILEGES FOR USER dbt_runner` auto-grants SELECT to `grafana_read` on any new table dbt creates.

## CI Security Scanning

Trivy scans run automatically in CI on push/PR and weekly. To scan locally:
```bash
trivy image --scanners vuln --severity CRITICAL,HIGH --ignore-unfixed streaming_system-producer
```

CVE exceptions are tracked in `.trivyignore` with `Added:` and `Expires:` dates for quarterly review.

## Known Limitations

This is a single-machine development/thesis deployment. For a detailed security analysis including known gaps (no TLS, default credentials, unencrypted backups), see [TECHNICAL_DOCUMENTATION.md](TECHNICAL_DOCUMENTATION.md#7-security-model).
