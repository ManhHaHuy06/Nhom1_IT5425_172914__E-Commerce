#!/bin/sh
set -e
mc alias set lake http://minio:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
mc mb --ignore-existing "lake/$MINIO_BUCKET"
for p in bronze silver gold _quarantine; do
  echo -n "" | mc pipe "lake/$MINIO_BUCKET/$p/.keep"
done
mc ls "lake/$MINIO_BUCKET"
