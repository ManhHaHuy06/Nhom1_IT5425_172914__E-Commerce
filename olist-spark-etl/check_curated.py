import os
import pandas as pd

import boto3
from io import BytesIO

ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "minioadmin"
BUCKET = "olist"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

def read_parquet(prefix):
    objects = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=prefix
    )

    dfs = []

    for obj in objects.get("Contents", []):
        key = obj["Key"]

        if key.endswith(".parquet"):
            print("Reading:", key)

            data = s3.get_object(
                Bucket=BUCKET,
                Key=key
            )["Body"].read()

            dfs.append(pd.read_parquet(BytesIO(data)))

    return pd.concat(dfs, ignore_index=True)


geo = read_parquet("curated/dim_geolocation/")
customer = read_parquet("curated/dim_customer/")

print("\n===== GEOLOCATION =====")
print(geo.dtypes)
print(geo["geolocation_key"].head(10).tolist())

print("\n===== CUSTOMER =====")
print(customer.dtypes)
print(customer["geolocation_key"].head(10).tolist())

geo_keys = set(
    geo["geolocation_key"]
    .dropna()
    .astype("int64")
)

customer_keys = set(
    customer["geolocation_key"]
    .dropna()
    .astype("int64")
)

orphans = customer_keys - geo_keys

print("\n===== RESULT =====")
print("Geo keys:", len(geo_keys))
print("Customer geo keys:", len(customer_keys))
print("Orphan keys:", len(orphans))

print("\nFirst 20 orphan keys:")
print(list(orphans)[:20])