#!/bin/bash
docker run -it -p 8080:8080 \
  --env "_AIRFLOW_DB_MIGRATE=true" \
  --env "_AIRFLOW_WWW_USER_CREATE=true" \
  --env "_AIRFLOW_WWW_USER_PASSWORD=admin" \
    airflow_custom:0.4 standalone
