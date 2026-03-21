COMPOSE = docker compose

CORE_SERVICES = postgres kafka spark-stream grafana producer dbt dbt-scheduler alert-receiver
OPS_SERVICES  = backup-cron retention kafka-lag

.PHONY: real dev ps health logs logs-core restart down downv rebuild \
        backup restore reset-system reset-restore \
        backupd backupd-off backup-logs \
        dbt-version dbt-deps dbt-debug dbt-build

# -------------------------
# Start modes
# -------------------------

# REAL: core + ops (including backup-cron)
real:
	$(COMPOSE) --profile ops up -d
	@$(COMPOSE) ps

# DEV: dev + ops (pgadmin, kafka-ui + backup-cron)
dev:
	$(COMPOSE) --profile dev --profile ops up -d
	@$(COMPOSE) ps

# Show all containers (including stopped)
ps:
	$(COMPOSE) ps

# Show health of key services only (filtered view)
health:
	@$(COMPOSE) ps --format "table {{.Name}}\t{{.State}}\t{{.Status}}" | grep -E "(postgres|kafka|spark-stream|producer|grafana|dbt|dbt-scheduler|alert-receiver|backup-cron|retention|kafka-lag)" || true

logs:
	$(COMPOSE) logs -f --tail=200

logs-core:
	$(COMPOSE) logs -f --tail=200 $(CORE_SERVICES)

restart:
	$(COMPOSE) restart
	@$(COMPOSE) ps

# -------------------------
# Stop / cleanup
# -------------------------

# Stop everything (all profiles)
down:
	$(COMPOSE) --profile dev --profile ops down --remove-orphans

# Stop + remove volumes (all profiles) (DESTROYS Postgres data volume!)
downv:
	$(COMPOSE) --profile dev --profile ops down -v --remove-orphans

rebuild:
	$(COMPOSE) build --no-cache

# -------------------------
# Backups (manual + daemon)
# Notes:
# - Backups are saved on the host in ./backups (mounted into containers as /backups)
# - The backup daemon (backup-cron) runs every 2 hours
# -------------------------

# Manual one-off backup (on-demand) executed inside the postgres container
backup:
	$(COMPOSE) exec postgres sh -lc '\
	set -e; \
	ts=$$(date +%Y%m%d_%H%M); \
	echo "Creating backup: backup_$${ts}.dump"; \
	pg_dump -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -F c -f "/backups/backup_$${ts}.dump"; \
	echo "Backup saved: /backups/backup_$${ts}.dump"; \
	'

# Restore from a chosen backup file
# Usage: make restore FILE=backup_YYYYMMDD_HHMM.dump
restore:
	@if [ -z "$(FILE)" ]; then echo "ERROR: FILE is required. Example: make restore FILE=backup_20260305_2200.dump"; exit 1; fi
	$(COMPOSE) exec postgres sh -lc '\
	set -e; \
	echo "Restoring /backups/$(FILE) into $$POSTGRES_DB ..."; \
	pg_restore -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" --clean --if-exists "/backups/$(FILE)"; \
	echo "Restore finished."; \
	'

# Ensure backup daemon is running
backupd:
	$(COMPOSE) --profile ops up -d backup-cron

# Stop backup daemon
backupd-off:
	$(COMPOSE) stop backup-cron

# Show last logs from backup daemon
backup-logs:
	$(COMPOSE) logs --tail=200 backup-cron

# -------------------------
# Full reset / recovery (reproducible demo flow)
# -------------------------

# Reset system (clean Docker state) - removes volumes (including Postgres data!)
reset-system: downv
	$(COMPOSE) --profile ops up -d
	@$(COMPOSE) ps

# Reset system + restore DB + rebuild dbt
# Usage: make reset-restore FILE=backup_YYYYMMDD_HHMM.dump
reset-restore: downv
	@if [ -z "$(FILE)" ]; then echo "ERROR: FILE is required. Example: make reset-restore FILE=backup_20260305_2200.dump"; exit 1; fi
	$(COMPOSE) --profile ops up -d
	@echo "Waiting for Postgres to become ready..."
	@sleep 10
	$(MAKE) restore FILE=$(FILE)
	$(MAKE) dbt-build
	@$(COMPOSE) ps

# -------------------------
# dbt
# -------------------------

# Show dbt and adapter versions
dbt-version:
	$(COMPOSE) exec dbt sh -lc 'cd /dbt && dbt --version'

# Install / update dbt packages (run after changing packages.yml)
dbt-deps:
	$(COMPOSE) exec dbt sh -lc 'cd /dbt && dbt deps'

# Validate dbt profile and database connection
dbt-debug:
	$(COMPOSE) exec dbt sh -lc 'cd /dbt && dbt debug'

# Build models + run data tests (main daily command)
dbt-build:
	$(COMPOSE) exec dbt sh -lc 'cd /dbt && dbt build'