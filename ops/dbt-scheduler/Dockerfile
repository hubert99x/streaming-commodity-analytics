FROM ghcr.io/dbt-labs/dbt-postgres:1.8.2

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      cron bash procps jq postgresql-client \
 && rm -rf /var/lib/apt/lists/*

# Force a patched DeepDiff version to avoid CVE-2025-58367
RUN pip install --no-cache-dir --upgrade "deepdiff>=8.6.1"

WORKDIR /dbt

COPY run_dbt_run.sh  /ops/run_dbt_run.sh
COPY run_dbt_test.sh /ops/run_dbt_test.sh
COPY start.sh        /ops/start.sh

RUN chmod +x /ops/run_dbt_run.sh /ops/run_dbt_test.sh /ops/start.sh

# Force shell entrypoint to avoid the default "dbt" entrypoint
ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["/ops/start.sh"]