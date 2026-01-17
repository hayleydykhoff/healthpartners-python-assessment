"""
Daily ingestion job for CMS Provider Data (Hospital-themed datasets).

This script:
- Retrieves dataset metadata from the CMS provider data metastore
- Identifies datasets related to the "Hospitals" theme
- Downloads and processes CSV files in parallel
- Normalizes column names to snake_case
- Tracks dataset modification timestamps to support incremental daily runs

Designed to run on a standard Windows or Linux machine without
Databricks, Spark, or cloud-specific dependencies.
"""

# ---------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------
import os
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------
# Third-party imports
# ---------------------------------------------------------------------
import requests
import pandas as pd

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
OUTPUT_DIR = "output"
METADATA_FILE = "metadata.json"
MAX_WORKERS = 5


# ---------------------------------------------------------------------
# Metadata persistence helpers
# ---------------------------------------------------------------------
def load_metadata():
    """
    Load dataset modification metadata from disk.

    The metadata file tracks the most recent modification timestamp
    for each dataset processed in prior runs.
    """
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {"datasets": {}}


def save_metadata(metadata):
    """
    Persist updated dataset modification metadata to disk.
    """
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)


# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------
def to_snake_case(column_name: str) -> str:
    """
    Convert column names with mixed case, spaces, and special characters
    to snake_case.

    Example:
    "Patients’ rating of the facility linear mean score"
    -> "patients_rating_of_the_facility_linear_mean_score"
    """
    col = column_name.lower()
    col = re.sub(r"[’']", "", col)
    col = re.sub(r"[^a-z0-9]+", "_", col)
    return col.strip("_")


# ---------------------------------------------------------------------
# CMS API interaction
# ---------------------------------------------------------------------
def fetch_datasets():
    """
    Retrieve the full list of datasets from the CMS provider data metastore.
    """
    response = requests.get(METASTORE_URL)
    response.raise_for_status()
    return response.json().get("items", [])


def is_hospital_dataset(dataset):
    """
    Determine whether a dataset is related to hospitals
    based on its theme metadata.
    """
    theme = dataset.get("theme", "")
    return "hospital" in theme.lower()


def needs_update(dataset, metadata):
    """
    Determine whether a dataset should be downloaded and processed.

    A dataset requires processing if it has not been seen before
    or if it has been modified since the previous run.
    """
    dataset_id = dataset.get("identifier")
    last_modified = dataset.get("modified")

    if dataset_id not in metadata["datasets"]:
        return True

    return last_modified > metadata["datasets"][dataset_id]


# ---------------------------------------------------------------------
# Dataset processing logic
# ---------------------------------------------------------------------
def process_dataset(dataset, metadata):
    """
    Download, clean, and persist a single CMS dataset.

    - Downloads the dataset CSV
    - Normalizes column names to snake_case
    - Writes the cleaned file to disk
    - Updates metadata with the dataset's last modified timestamp
    """
    dataset_id = dataset.get("identifier")
    title = dataset.get("title", dataset_id)
    last_modified = dataset.get("modified")

    # CMS datasets may have multiple distributions; use the first CSV
    download_url = dataset.get("distribution", [{}])[0].get("downloadURL")
    if not download_url:
        return None

    print(f"Processing dataset: {title}")

    df = pd.read_csv(download_url)
    df.columns = [to_snake_case(c) for c in df.columns]

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, f"{dataset_id}.csv")
    df.to_csv(output_path, index=False)

    metadata["datasets"][dataset_id] = last_modified
    return output_path


# ---------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------
def main():
    """
    Orchestrate daily ingestion of hospital-related CMS datasets.
    """
    metadata = load_metadata()
    datasets = fetch_datasets()

    # Filter for hospital-related datasets requiring processing
    hospital_datasets = [
        d for d in datasets
        if is_hospital_dataset(d) and needs_update(d, metadata)
    ]

    if not hospital_datasets:
        print("No new or updated hospital datasets found.")
        return

    # Thread-based parallelism is appropriate here since work is I/O-bound
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_dataset, d, metadata)
            for d in hospital_datasets
        ]

        for future in as_completed(futures):
            result = future.result()
            if result:
                print(f"Saved cleaned file to {result}")

    save_metadata(metadata)
    print("Ingestion complete.")


if __name__ == "__main__":
    main()
