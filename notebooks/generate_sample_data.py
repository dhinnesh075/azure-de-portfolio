"""
Creates synthetic multi-source claim files with deliberately inconsistent
schemas (different column names/order), mimicking the kind of messy
multi-source Excel ingestion described in the resume's rebate-processing
pipeline. Run this once to get local test data before wiring up ADLS Gen2.
"""
import random
from datetime import date, timedelta

import pandas as pd

OUT_DIR = "data/raw"
import os
os.makedirs(OUT_DIR, exist_ok=True)

random.seed(42)
start = date(2026, 9, 1)

# Source A: one naming convention
rows_a = []
for i in range(50):
    d = start + timedelta(days=random.randint(0, 20))
    rows_a.append({
        "ClaimID": f"A-{1000 + i}",
        "ClaimDate": d.isoformat(),
        "Amount": round(random.uniform(50, 5000), 2),
        "Status": random.choice(["Approved", "Pending", "Denied"]),
    })
pd.DataFrame(rows_a).to_excel(f"{OUT_DIR}/source_a.xlsx", index=False)

# Source B: different column names/order, same meaning
rows_b = []
for i in range(40):
    d = start + timedelta(days=random.randint(0, 20))
    rows_b.append({
        "claim_id": f"B-{2000 + i}",
        "status": random.choice(["Approved", "Pending", "Denied"]),
        "amount_usd": round(random.uniform(50, 5000), 2),
        "claim_date": d.isoformat(),
    })
pd.DataFrame(rows_b).to_excel(f"{OUT_DIR}/source_b.xlsx", index=False)

print(f"Wrote {OUT_DIR}/source_a.xlsx and {OUT_DIR}/source_b.xlsx")
