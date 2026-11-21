#!/bin/sh
set -eu
attempt=0
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 60 ]; then echo 'MinIO initialization failed' >&2; exit 1; fi
  sleep 2
done
mc mb --ignore-existing local/herbintel-data
mc anonymous set none local/herbintel-data
mc version enable local/herbintel-data
mc admin user add local "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
mc admin policy create local herb-service /init/service-policy.json
mc admin policy attach local herb-service --user "$S3_ACCESS_KEY"
mc ilm import local/herbintel-data < /init/lifecycle.json
printf ready | mc pipe local/herbintel-data/.ready
