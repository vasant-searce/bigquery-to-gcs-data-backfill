#  BigQuery to GCS - Batch Export Pipeline

This project provides a scalable batch pipeline to export historical data from **Google BigQuery** to **Google Cloud Storage (GCS)** as CSV files, while preserving Unix timestamps and organizing data into date-partitioned folders.

---

##  Problem Statement

Exporting large volumes of historical data from BigQuery often leads to:

* Loss of timestamp precision
* Poor file organization
* Manual effort in splitting data by date

This solution automates the entire process with structured, efficient exports.

---

##  Architecture Overview

###  Data Flow

1. BigQuery Tables
2. Python Script / Cloud Function
3. Google Cloud Storage

---

##  Tools & Technologies

* Google BigQuery
* Google Cloud Storage (GCS)
* Python
* Cloud Functions (Serverless option)
* Google Cloud SDK

---

##  Key Features

*  Export BigQuery tables to CSV
*  Preserve INT64 Unix timestamps (no conversion)
*  Organize files by date (`YYYY/MM/DD/`)
*  Supports large datasets
*  Configurable date range filtering
*  Multi-table export support
*  Dual deployment:

  * Standalone script
  * Cloud Function

---

##  How It Works

### 1️ Query BigQuery Data

* Filters data using Unix timestamp (INT64)
* Uses `TIMESTAMP_MILLIS()` only for grouping
* Keeps original timestamp intact

---

### 2️ Group Data by Date

* Converts timestamp → date (for folder structure)
* Groups records per day

---

### 3️ Export to GCS

* Each day exported as separate CSV
* Stored as:

```
gs://bucket/YYYY/MM/DD/table_name.csv
```

---

##  Output Structure

```
gs://your-bucket/
└── 2026/
    └── 02/
        ├── 01/
        │   └── table.csv
        ├── 02/
        │   └── table.csv
        └── ...
```

---

##  Example Output

```csv
event_id,event_type,generated_at
EVT-001,ALERT,1706918400000
EVT-002,INFO,1706918460000
```

 **Note:** `generated_at` remains INT64 (Unix milliseconds)

---

##  Deployment Options

###  Option 1: Standalone Script

```bash id="n3"
python standalone_export.py
```

* Run locally or via Cloud Shell
* Best for ad-hoc or backfill jobs

---

###  Option 2: Cloud Function

* HTTP-triggered serverless execution
* Ideal for scheduled exports

```bash id="n4"
gcloud functions deploy export-event-data \
  --gen2 \
  --runtime=python311 \
  --trigger-http
```

---

##  Required Permissions

### BigQuery

* `roles/bigquery.dataViewer`
* `roles/bigquery.jobUser`

### GCS

* `roles/storage.objectCreator`

---

##  Prerequisites

* GCP project with BigQuery & GCS enabled
* Python 3.7+
* Google Cloud SDK configured

---

##  Installation

```bash id="n5"
pip install google-cloud-bigquery google-cloud-storage
```

---

##  Key Learnings

* Preserving Unix timestamps avoids downstream inconsistencies
* Date-based partitioning improves data organization
* Cloud Functions enable scalable serverless execution
* BigQuery + GCS integration is highly efficient for batch exports

---

##  Use Cases

* Historical data backfill
* Data lake creation
* Archival pipelines
* Downstream ETL processing

---

##  Future Improvements

* Parquet/Avro support
* Incremental exports
* Metadata tracking
* Logging & monitoring integration

---

##  Author

**Vasanth S**

---

##  Support

If you found this useful, give it a ⭐ and feel free to contribute!
