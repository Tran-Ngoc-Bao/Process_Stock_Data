#!/usr/bin/env bash
set -euo pipefail

if [ -f /opt/airflow/code/trino ]; then
  chmod +x /opt/airflow/code/trino || true
fi

exec /entrypoint "$@"