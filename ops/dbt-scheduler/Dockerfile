FROM ghcr.io/dbt-labs/dbt-postgres:1.9.0

USER root

ENV DEBIAN_FRONTEND=noninteractive

# Update base OS packages first, then install only required runtime tools.
RUN apt-get update \
 && apt-get upgrade -y \
 && apt-get dist-upgrade -y \
 && apt-get install -y --no-install-recommends \
      bash \
      jq \
      postgresql-client \
 && apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade "deepdiff==8.6.1"

WORKDIR /dbt

COPY run_dbt_run.sh  /ops/run_dbt_run.sh
COPY run_dbt_test.sh /ops/run_dbt_test.sh
COPY scheduler.py    /ops/scheduler.py
COPY start.sh        /ops/start.sh

RUN chmod 0755 /ops/run_dbt_run.sh /ops/run_dbt_test.sh /ops/start.sh \
 && mkdir -p /tmp/dbt_target /tmp/dbt_logs /tmp/dbt_packages \
 && chown -R 1000:1000 /dbt /ops /tmp/dbt_target /tmp/dbt_logs /tmp/dbt_packages

USER 1000

ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["/ops/start.sh"]
