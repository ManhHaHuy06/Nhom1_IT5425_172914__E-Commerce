#!/bin/sh
set -e
mc alias set lake http://minio:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"
mc mb --ignore-existing "lake/$MINIO_BUCKET"
for p in bronze silver gold _quarantine; do
  echo -n "" | mc pipe "lake/$MINIO_BUCKET/$p/.keep"
done

# Read-only user for downstream readers (olist-lakehouse Spark reads bronze/ with it: SOURCE_S3_* in its .env)
# Built-in "readonly" has no s3:ListBucket (Spark must list prefixes), so use a policy scoped to this bucket.
if [ -n "$LAKE_READER_ACCESS_KEY" ] && [ -n "$LAKE_READER_SECRET_KEY" ]; then
  cat > /tmp/lake-reader.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetBucketLocation", "s3:ListBucket"], "Resource": ["arn:aws:s3:::$MINIO_BUCKET"]},
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::$MINIO_BUCKET/*"]}
  ]
}
JSON
  mc admin user add lake "$LAKE_READER_ACCESS_KEY" "$LAKE_READER_SECRET_KEY"
  mc admin policy create lake lake-reader /tmp/lake-reader.json
  mc admin policy detach lake readonly --user "$LAKE_READER_ACCESS_KEY" >/dev/null 2>&1 || true
  # attach fails if already attached (re-run) -> accept only that case
  mc admin policy attach lake lake-reader --user "$LAKE_READER_ACCESS_KEY"     || mc admin user info lake "$LAKE_READER_ACCESS_KEY" | grep -q "lake-reader"
  echo "read-only user $LAKE_READER_ACCESS_KEY ready (policy lake-reader on $MINIO_BUCKET)"
fi
mc ls "lake/$MINIO_BUCKET"
