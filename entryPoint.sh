#!/bin/bash
set -e

# Initialize DB (only the first time)
airflow db init

# Create admin user (idempotent)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Finally exec the command passed to the container (e.g. webserver)
exec airflow "$@"
