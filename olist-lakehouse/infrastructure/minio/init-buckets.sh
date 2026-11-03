#!/bin/sh
set -e

MINIO_HOST="${MINIO_ENDPOINT}"
MINIO_USER="${MINIO_ROOT_USER}"
MINIO_PASS="${MINIO_ROOT_PASSWORD}"

echo "Waiting for MinIO at ${MINIO_HOST}..."
MAX_RETRIES=30
RETRY_COUNT=0

# Vòng lặp chờ MinIO sẵn sàng
while [ "$RETRY_COUNT" -lt "$MAX_RETRIES" ]; do
    if mc alias set local "$MINIO_HOST" "$MINIO_USER" "$MINIO_PASS" > /dev/null 2>&1; then
        echo "MinIO ready!"
        break
    fi
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 2
done

# Timeout Handling
if [ "$RETRY_COUNT" -eq "$MAX_RETRIES" ]; then
    echo "ERROR: MinIO did not become ready"
    exit 1
fi

# handoff   : crawler CSV landing (bronze/orders/..., bronze/products/...)
# lakehouse : Iceberg warehouse (bronze/olist_*, silver/*, gold/*, exports/)
for bucket in handoff lakehouse; do
    if ! mc ls "local/${bucket}" > /dev/null 2>&1; then
        mc mb "local/${bucket}"
        echo "Bucket ${bucket} created"
    else
        echo "Bucket ${bucket} already exists"
    fi
done

# Remove legacy bucket names from older layouts.
for legacy in olist warehouse; do
    if mc ls "local/${legacy}" > /dev/null 2>&1; then
        mc rb --force "local/${legacy}"
        echo "Removed legacy bucket ${legacy}"
    fi
done

echo "MinIO init complete!"
mc ls local/
