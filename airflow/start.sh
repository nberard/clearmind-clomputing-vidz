#!/usr/bin/env bash
# Airflow container entrypoint: prepare the database, create or update the
# admin account from .env (AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD), then
# start Airflow in standalone mode.
set -euo pipefail

echo "Preparing the Airflow database (first start can take 1-2 min)…"
airflow db migrate >/dev/null

if [[ -n "${AIRFLOW_ADMIN_USER:-}" && -n "${AIRFLOW_ADMIN_PASSWORD:-}" ]]; then
  # Each airflow command takes 15-30 s on a small VPS, so only create the
  # account when it's missing. To change the password later:
  #   docker compose exec airflow airflow users delete -u <user>  then restart.
  users="$(airflow users list --output plain 2>/dev/null || true)"
  if awk '{print $2}' <<<"$users" | grep -qx "$AIRFLOW_ADMIN_USER"; then
    echo "Airflow account '$AIRFLOW_ADMIN_USER' already exists"
  else
    airflow users create --role Admin \
      --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD" \
      --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-Admin}" --lastname "${AIRFLOW_ADMIN_LASTNAME:-User}" \
      --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" >/dev/null
    echo "Airflow account '$AIRFLOW_ADMIN_USER' created"
  fi
fi

exec airflow standalone
