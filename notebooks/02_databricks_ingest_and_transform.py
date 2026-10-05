# Databricks notebook source
# Same logic as 01_ingest_and_transform.py, adapted to run inside a Databricks
# notebook against the real ADLS Gen2 containers via Unity Catalog external
# locations. `spark` and `dbutils` are provided automatically by Databricks.

import io
import json

import pandas as pd
from pyspark.sql import functions as F

STORAGE_ACCOUNT = "stdeportfolio28861"
RAW_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
CURATED_PATH = f"abfss://curated@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
CONTROL_PATH = f"abfss://control@{STORAGE_ACCOUNT}.dfs.core.windows.net/watermark.json"

SCHEMA_MAP = {
    "source_a.xlsx": {
        "ClaimID": "claim_id",
        "ClaimDate": "claim_date",
        "Amount": "amount",
        "Status": "status",
    },
    "source_b.xlsx": {
        "claim_id": "claim_id",
        "claim_date": "claim_date",
        "amount_usd": "amount",
        "status": "status",
    },
}

# --- 1. Read multi-source files and normalize to a common schema ---
# pandas can't read an abfss:// path directly, and Shared clusters block
# direct local-filesystem access (e.g. dbutils.fs.cp to /tmp) for isolation.
# Instead, read the file as raw bytes through Spark's binaryFile source
# (which uses the Unity Catalog credential) and hand those bytes to pandas
# in memory.
normalized_frames = []
for filename, rename_map in SCHEMA_MAP.items():
    binary_df = spark.read.format("binaryFile").load(RAW_PATH + filename)
    content = binary_df.select("content").head()[0]
    pdf = pd.read_excel(io.BytesIO(content))
    pdf = pdf.rename(columns=rename_map)[["claim_id", "claim_date", "amount", "status"]]
    pdf["source_file"] = filename
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.withColumn("claim_date", F.to_date("claim_date"))
    normalized_frames.append(sdf)

# --- 2. Union into a single consolidated DataFrame ---
consolidated = normalized_frames[0]
for frame in normalized_frames[1:]:
    consolidated = consolidated.unionByName(frame)

print(f"Total records after union: {consolidated.count()}")

# --- 3. Incremental load: filter to records newer than the last watermark ---
try:
    last_watermark = json.loads(dbutils.fs.head(CONTROL_PATH))["last_loaded_date"]
except Exception:
    last_watermark = None

if last_watermark:
    print(f"Incremental load: only claim_date > {last_watermark}")
    consolidated = consolidated.filter(F.col("claim_date") > last_watermark)
else:
    print("No watermark found — running full load")

new_record_count = consolidated.count()
print(f"Records to load this run: {new_record_count}")

# --- 4. Write analytics-ready Parquet output and update the watermark ---
if new_record_count > 0:
    consolidated.write.mode("append").partitionBy("status").parquet(CURATED_PATH)

    new_watermark = consolidated.agg(F.max("claim_date")).collect()[0][0]
    dbutils.fs.put(CONTROL_PATH, json.dumps({"last_loaded_date": str(new_watermark)}), overwrite=True)
    print(f"Updated watermark to {new_watermark}")
else:
    print("Nothing new to load; watermark unchanged")
