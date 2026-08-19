# dbt-scheduler image — built on a clean python:3.14-slim (Debian trixie)
# instead of dbt-labs/dbt-postgres to avoid stale Debian bullseye CVEs in the
# upstream image and to keep the base on a currently-supported Debian release.
FROM python:3.14-slim

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

# Bump these four together: dbt-core 1.12 requires dbt-adapters >=1.24.5 and
# dbt-common >=1.37.5, so pinning them separately lets the resolver pick
# incompatible transitive versions. Mirrored in .github/workflows/dbt-ci.yml.
RUN pip install --no-cache-dir \
      "dbt-core==1.12.2" \
      "dbt-adapters==1.24.5" \
      "dbt-common==1.39.0" \
      "dbt-postgres==1.11.0"

# deepdiff is installed separately with --upgrade so pip overrides
# dbt-common's deepdiff<9.0 constraint. dbt-common only uses the
# DeepDiff() constructor with ignore_order/verbose_level — the 9.x
# API is fully compatible with these calls (verified with dbt build
# PASS=69 on the 9.0 bump; 9.1 keeps the same constructor API).
# setuptools/wheel/urllib3/jaraco.context are upgraded to clear known CVEs in
# the transitive deps pulled by dbt-postgres.
RUN pip install --no-cache-dir --upgrade \
      "deepdiff==9.1.0" \
      setuptools wheel urllib3 "jaraco.context"

RUN groupadd -r -g 1000 dbtuser \
 && useradd  -r -u 1000 -g dbtuser -d /dbt -s /bin/bash dbtuser

WORKDIR /dbt

COPY run_dbt_test.sh /ops/run_dbt_test.sh
COPY scheduler.py    /ops/scheduler.py
COPY start.sh        /ops/start.sh

RUN chmod 0755 /ops/run_dbt_test.sh /ops/start.sh \
 && mkdir -p /tmp/dbt_target /tmp/dbt_logs /tmp/dbt_packages \
 && chown -R 1000:1000 /dbt /ops /tmp/dbt_target /tmp/dbt_logs /tmp/dbt_packages

USER 1000

ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["/ops/start.sh"]
