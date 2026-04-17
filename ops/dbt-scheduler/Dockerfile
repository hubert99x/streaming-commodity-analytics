# dbt-scheduler image — built on a clean python:3.13-slim (Debian trixie)
# instead of dbt-labs/dbt-postgres to avoid stale Debian bullseye CVEs in the
# upstream image and to keep the base on a currently-supported Debian release.
FROM python:3.13-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get upgrade -y \
 && apt-get install -y --no-install-recommends \
      bash \
      jq \
      postgresql-client \
 && apt-get autoremove -y \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# dbt 1.10 is the current LTS line. dbt-adapters / dbt-common stay on
# the same versions as before (both compatible with 1.10.x). Bump these
# together to avoid resolver picking incompatible transitive deps.
RUN pip install --no-cache-dir \
      "dbt-core==1.10.20" \
      "dbt-adapters==1.22.10" \
      "dbt-common==1.37.3" \
      "dbt-postgres==1.10.0"

# deepdiff 8.6.2 is installed separately with --upgrade so pip overrides
# dbt-common's deepdiff<8.0 constraint (the project's scheduler relies on the
# 8.x API). Same approach as the upstream Dockerfile.
# setuptools/wheel/urllib3/jaraco.context are upgraded to clear known CVEs in
# the transitive deps pulled by dbt-postgres.
RUN pip install --no-cache-dir --upgrade \
      "deepdiff==8.6.2" \
      setuptools wheel urllib3 "jaraco.context"

RUN groupadd -r -g 1000 dbtuser \
 && useradd  -r -u 1000 -g dbtuser -d /dbt -s /bin/bash dbtuser

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
