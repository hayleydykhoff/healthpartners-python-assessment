# CMS Hospital Dataset Ingestion

This repository contains a Python script that ingests hospital-related datasets from the CMS Provider Data metastore.

The job is designed to be run daily and will:
- Retrieve dataset metadata from the CMS Provider Data API
- Identify datasets related to the "Hospitals" theme
- Download and process CSV datasets in parallel
- Normalize column names to snake_case
- Track dataset modification timestamps to support incremental runs

## Approach

The script queries the CMS metastore endpoint to retrieve dataset metadata and filters datasets based on the "Hospitals" theme. For each dataset, the job compares the dataset’s last modified timestamp against locally stored metadata to determine whether the dataset should be reprocessed.

Processed datasets are written to disk as cleaned CSV files, and metadata is updated to reflect the most recent successful run.

Parallel processing is implemented using a thread pool, which is appropriate for this I/O-bound workload.

## Incremental Processing

Incremental ingestion is implemented using a local metadata file (`metadata.json`) that tracks the most recent modification timestamp for each dataset processed. On subsequent runs, only new or updated datasets are downloaded and processed.

This approach allows the script to be safely scheduled for daily execution using cron (Linux/macOS) or Task Scheduler (Windows).

## Requirements

The script is written in Python and is designed to run on a standard Windows or Linux machine.

Required Python packages:
- `requests`
- `pandas`

Install dependencies with:
```bash
pip install -r requirements.txt
