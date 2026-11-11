#!/usr/bin/env bash
# Airflow container entrypoint: prepare the database, create or update the
# admin account from .env (AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD), then
# start Airflow in standalone mode.
set -euo pipefail

airflow db migrate >/dev/null

if [[ -n "${AIRFLOW_ADMIN_USER:-}" && -n "${AIRFLOW_ADMIN_PASSWORD:-}" ]]; then
  # Recreate the account so a password changed in .env applies on restart.
  if airflow users list --output plain 2>/dev/null | awk '{print $2}' | grep -qx "$AIRFLOW_ADMIN_USER"; then
    airflow users delete --username "$AIRFLOW_ADMIN_USER" >/dev/null
  fi
  airflow users create --role Admin \
    --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" >/dev/null
  echo "Airflow account '$AIRFLOW_ADMIN_USER' ready"
fi

exec airflow standalone
