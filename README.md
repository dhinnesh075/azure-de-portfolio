# Azure Data Engineering Portfolio Project

End-to-end ETL pipeline built to learn (and demonstrate) the core Azure Data
Engineer toolset: ADLS Gen2, Azure Databricks (PySpark), Azure Data Factory,
Synapse, and incremental load patterns.

## Architecture

```
Multi-source files (Excel/CSV)
        |
        v
  ADLS Gen2 [raw]  --(schema normalize + union + watermark filter)-->  PySpark (Databricks)
        |
        v
  ADLS Gen2 [curated]  (Parquet)
        |
        v
  Synapse serverless SQL (external table)  -->  Power BI
```

Orchestrated end-to-end by an Azure Data Factory pipeline that triggers the
Databricks notebook on a schedule and alerts on failure.

## Azure resources

- Resource group: `rg-de-portfolio` (Central India)
- Storage account: ADLS Gen2, hierarchical namespace enabled
- Containers:
  - `raw` — landing zone for incoming source files
  - `curated` — transformed, analytics-ready Parquet output
  - `control` — watermark/control table tracking last successful load per source

## Status

- [x] Azure account + resource group + ADLS Gen2 storage provisioned
- [ ] Local PySpark transform logic (schema normalize + union + watermark)
- [ ] Databricks workspace connected to ADLS Gen2
- [ ] ADF pipeline triggering the Databricks notebook
- [ ] Synapse serverless external table over curated Parquet
- [ ] Power BI report
- [ ] ADF failure alerting via Logic App

## Local development

```bash
python3 notebooks/generate_sample_data.py   # creates synthetic multi-source files
python3 notebooks/01_ingest_and_transform.py
```
