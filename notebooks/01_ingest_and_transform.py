"""
Core transform logic: schema normalization, union across sources, and
incremental (watermark) load. Runs locally against data/raw and data/curated
today; swap the RAW_PATH/CURATED_PATH/CONTROL_PATH constants for
abfss://... paths once this runs inside a Databricks notebook against ADLS
Gen2, and this logic doesn't otherwise change.
"""
import json
import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = "data/raw"
CURATED_PATH = "data/curated"
CONTROL_FILE = "data/control/watermark.json"

spark = SparkSession.builder.appName("claims-ingest").master("local[*]").getOrCreate()

# --- 1. Read multi-source files and normalize to a common schema ---
# Each source has different column names/order; map them all to one
# canonical schema before anything else touches the data.

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

normalized_frames = []
for filename, rename_map in SCHEMA_MAP.items():
    path = os.path.join(RAW_PATH, filename)
    pdf = pd.read_excel(path)
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
os.makedirs(os.path.dirname(CONTROL_FILE), exist_ok=True)
if os.path.exists(CONTROL_FILE):
    with open(CONTROL_FILE) as f:
        last_watermark = json.load(f).get("last_loaded_date")
else:
    last_watermark = None

if last_watermark:
    print(f"Incremental load: only claim_date > {last_watermark}")
    consolidated = consolidated.filter(F.col("claim_date") > last_watermark)
else:
    print("No watermark found — running full load")

new_record_count = consolidated.count()
print(f"Records to load this run: {new_record_count}")

# --- 4. Write analytics-ready Parquet output ---
if new_record_count > 0:
    consolidated.write.mode("append").partitionBy("status").parquet(CURATED_PATH)

    new_watermark = consolidated.agg(F.max("claim_date")).collect()[0][0]
    with open(CONTROL_FILE, "w") as f:
        json.dump({"last_loaded_date": str(new_watermark)}, f)
    print(f"Updated watermark to {new_watermark}")
else:
    print("Nothing new to load; watermark unchanged")

spark.stop()
