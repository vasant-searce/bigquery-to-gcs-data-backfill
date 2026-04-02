# BigQuery to GCS Batch Export

Export historical BigQuery data to Google Cloud Storage with Unix timestamp preservation and date-based folder organization.

## Overview

This project provides two deployment methods to export data from BigQuery tables to GCS buckets:

1. **Standalone Python Script** - Run directly from your local machine or Cloud Shell
2. **Cloud Function** - Deploy as a serverless HTTP-triggered function

Both methods preserve INT64 Unix timestamps and organize data in `year/month/day` folder structures.

## Features

 Exports BigQuery tables to GCS as CSV files  
 Preserves INT64 Unix timestamps (no conversion)  
 Organizes files by date: `YYYY/MM/DD/table_name.csv`  
 Handles large datasets efficiently  
 Configurable date range filtering  
 Supports multiple tables (configurable)  

## Table of Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Method 1: Standalone Script](#method-1-standalone-script) - `bq-historic-backfill.py`
  - [Method 2: Cloud Function](#method-2-cloud-function) - `bq-gcs-backup-csv.py`
- [Output Structure](#output-structure)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│  BigQuery   │────────▶│    Script    │────────▶│     GCS     │
│   Tables    │         │  /Function   │         │   Bucket    │
└─────────────┘         └──────────────┘         └─────────────┘
                                │
                                ▼
                    Preserves INT64 timestamps
                    Organizes by year/month/day
```

**Data Flow:**
1. Query BigQuery table with Unix timestamp filter
2. Group records by date (using `TIMESTAMP_MILLIS()` for date calculation)
3. Export each day's data as separate CSV file
4. Upload to GCS with folder structure: `gs://bucket/YYYY/MM/DD/table.csv`

**Key Feature:** Unix timestamps remain as INT64 in CSV (e.g., `1706918400000`), not converted to datetime strings.

---

## Prerequisites

### For Standalone Script:
- Python 3.7 or higher
- Google Cloud SDK installed and authenticated
- Required Python packages: `google-cloud-bigquery`, `google-cloud-storage`

### For Cloud Function:
- GCP Project with Cloud Functions API enabled
- Sufficient IAM permissions (see [Permissions](#permissions))

### Permissions Required:

Your service account or user account needs:

```bash
# BigQuery permissions
roles/bigquery.dataViewer
roles/bigquery.jobUser

# GCS permissions
roles/storage.objectCreator
```

Grant permissions:

```bash
# For user account
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="user:YOUR-EMAIL@example.com" \
  --role="roles/bigquery.dataViewer"

# For service account (Cloud Functions)
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:SERVICE-ACCOUNT@PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# GCS bucket permissions
gsutil iam ch user:YOUR-EMAIL@example.com:objectCreator gs://BUCKET_NAME
```

---

## Installation

### Clone the Repository

```bash
git clone https://github.com/yourusername/bigquery-gcs-export.git
cd bigquery-gcs-export
```

### Install Dependencies (for Standalone Script)

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
google-cloud-bigquery==3.11.4
google-cloud-storage==2.10.0
```

### Authenticate with Google Cloud

```bash
gcloud auth application-default login
```

---

## Configuration

### Edit Configuration Variables

Open `standalone_export.py` or `main.py` (for Cloud Function) and update:

```python
# ============================================================================
# CONFIGURATION
# ============================================================================
PROJECT_ID = 'your-project-id'           # GCP Project ID
DATASET_ID = 'your_dataset'              # BigQuery dataset name
TABLE_NAME = 'event_data_packet'         # Table to export
TIMESTAMP_COLUMN = 'generated_at'        # Unix timestamp column (INT64)
GCS_BUCKET = 'your-gcs-bucket'           # GCS bucket name

# Date cutoff (optional - customize as needed)
YESTERDAY_END = datetime(2026, 2, 23, 23, 59, 59)
YESTERDAY_END_UNIX = int(YESTERDAY_END.timestamp() * 1000)  # Milliseconds
```

### Table Configurations (for Multi-Table Export)

```python
TABLE_CONFIGS = [
    {
        'table_name': 'asset_telemetry_data_packet',
        'timestamp_column': 'time',
        'enabled': True
    },
    {
        'table_name': 'event_data_packet',
        'timestamp_column': 'generated_at',
        'enabled': True
    }
]
```

---

## Usage

### Method 1: Standalone Script

#### Run Locally or in Cloud Shell

```bash
# Basic execution
python standalone_export.py
```

#### Expected Output

```
======================================================================
EVENT DATA PACKET EXPORT TO GCS
======================================================================
Project: gcp-poc-cloudisde
Dataset: prod_iot
Table: event_data_packet
Timestamp Column: generated_at
GCS Bucket: gcs-historic-backup
Cutoff: 2026-02-23 23:59:59
Cutoff Unix (ms): 1740337199000
======================================================================

Initializing BigQuery and GCS clients...

Executing BigQuery query...
Grouping data by date...
Total rows fetched: 125000
Date range spans: 22 days

Exporting to GCS...
  ✓ 2026/02/02/event_data_packet.csv - 5500 rows
  ✓ 2026/02/03/event_data_packet.csv - 6200 rows
  ✓ 2026/02/04/event_data_packet.csv - 5800 rows
  ...
  ✓ 2026/02/23/event_data_packet.csv - 5300 rows

======================================================================
EXPORT COMPLETED
Total Files: 22
Total Rows: 125000
GCS Bucket: gs://gcs-historic-backup
======================================================================
```

---

### Method 2: Cloud Function

#### Deploy via Cloud Console

1. Go to [Cloud Functions Console](https://console.cloud.google.com/functions)
2. Click **"CREATE FUNCTION"**
3. Configure:
   - **Environment:** 2nd gen
   - **Function name:** `export-event-data`
   - **Region:** `asia-south1` (or your preferred region)
   - **Trigger type:** HTTPS
   - **Authentication:** ✓ Allow unauthenticated invocations

4. Click **"NEXT"**

5. Code configuration:
   - **Runtime:** Python 3.11
   - **Entry point:** `export_event_data`
   - Paste `bq-gcs-backup-csv.py` into `main.py`
   - Paste requirements into `requirements.txt`:
     ```
     google-cloud-bigquery==3.11.4
     google-cloud-storage==2.10.0
     flask==3.0.0
     ```

6. Runtime settings (expand "Runtime, build, connections and security"):
   - **Memory:** 2 GiB
   - **Timeout:** 3600 seconds (1 hour)
   - **Maximum instances:** 10

7. Click **"DEPLOY"**

#### Deploy via gcloud CLI

```bash
gcloud functions deploy export-event-data \
  --gen2 \
  --runtime=python311 \
  --region=asia-south1 \
  --source=. \
  --entry-point=export_event_data \
  --trigger-http \
  --allow-unauthenticated \
  --timeout=3600 \
  --memory=2GB
```

#### Trigger the Cloud Function

**Via curl:**
```bash
curl -X POST "https://REGION-PROJECT.cloudfunctions.net/export-event-data" \
  -H "Content-Type: application/json"
```

**Via Testing Tab in Console:**
1. Go to your deployed function
2. Click **"TESTING"** tab
3. Leave payload empty or use `{}`
4. Click **"TEST THE FUNCTION"**

**Response:**
```json
{
  "status": "success",
  "table": "event_data_packet",
  "total_files": 22,
  "total_rows": 125000,
  "duration_seconds": 45.3,
  "cutoff_date": "2026-02-23 23:59:59",
  "bucket": "gs://gcs-historic-backup"
}
```

---

## Output Structure

### GCS Bucket Organization

```
gs://your-bucket-name/
├── 2026/
│   ├── 02/
│   │   ├── 02/
│   │   │   └── event_data_packet.csv
│   │   ├── 03/
│   │   │   └── event_data_packet.csv
│   │   ├── 04/
│   │   │   └── event_data_packet.csv
│   │   └── ...
│   │       └── 23/
│   │           └── event_data_packet.csv
```

### CSV File Format

**Preserves INT64 Unix timestamps:**

```csv
event_id,event_type,asset_id,severity,message,generated_at
EVT-001,ALERT,ASSET-001,HIGH,Temperature exceeded,1706918400000
EVT-002,INFO,ASSET-002,LOW,Normal operation,1706918460000
EVT-003,WARNING,ASSET-001,MEDIUM,Humidity high,1706918520000
```

The `generated_at` column remains as **INT64** (Unix timestamp in milliseconds), not converted to datetime strings.

### Verify Export

```bash
# List files
gsutil ls -r gs://your-bucket-name/2026/02/

# Download a sample
gsutil cp gs://your-bucket-name/2026/02/02/event_data_packet.csv ./sample.csv

# View contents
head -20 sample.csv
```

---

## How It Works

### 1. Query BigQuery with Timestamp Filter

```sql
SELECT 
    *,
    DATE(TIMESTAMP_MILLIS(generated_at)) as export_date
FROM `project.dataset.event_data_packet`
WHERE generated_at <= 1740337199000  -- Unix timestamp in milliseconds
ORDER BY generated_at
```

**Key Points:**
- `TIMESTAMP_MILLIS(generated_at)` converts INT64 to TIMESTAMP for date calculation
- Original `generated_at` value remains unchanged (INT64)
- `export_date` is a helper field removed before export

### 2. Group Records by Date

```python
data_by_date = {}
for row in results:
    row_dict = dict(row.items())
    export_date = row_dict.pop('export_date')  # Remove helper
    date_key = export_date.strftime('%Y/%m/%d')
    
    if date_key not in data_by_date:
        data_by_date[date_key] = []
    
    data_by_date[date_key].append(row_dict)
```

### 3. Export Each Day as Separate CSV

```python
for date_path, rows in sorted(data_by_date.items()):
    filename = f'{date_path}/{TABLE_NAME}.csv'
    # Write CSV with INT64 timestamps preserved
    blob.upload_from_string(csv_content)
```

---

## Troubleshooting

### Common Issues

#### 1. "Could not automatically determine credentials"

**Solution:**
```bash
gcloud auth application-default login
```

#### 2. "Permission denied" or "403 Forbidden"

**Solution:** Grant required IAM roles:
```bash
# BigQuery
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="user:YOUR-EMAIL@example.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="user:YOUR-EMAIL@example.com" \
  --role="roles/bigquery.jobUser"

# GCS
gsutil iam ch user:YOUR-EMAIL@example.com:objectCreator gs://BUCKET_NAME
```

#### 3. "Table not found" Error

**Check table exists:**
```bash
bq ls PROJECT_ID:DATASET_ID
bq show PROJECT_ID:DATASET_ID.TABLE_NAME
```

#### 4. "Unrecognized name: time" or Column Error

**Solution:** Verify the timestamp column name in your BigQuery table:
```sql
SELECT column_name, data_type
FROM `PROJECT.DATASET.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'event_data_packet'
ORDER BY ordinal_position
```

Update `TIMESTAMP_COLUMN` in config to match actual column name.

#### 5. Cloud Function "Service Unavailable"

**Possible causes:**
- Wrong entry point name (must be `export_event_data`)
- Missing Flask dependency in `requirements.txt`
- Function timeout (increase to 3600 seconds)
- Insufficient memory (increase to 2GB)

**Check logs:**
```bash
gcloud functions logs read export-event-data --region=REGION --limit=50
```

#### 6. "Bucket not found"

**Create bucket:**
```bash
gsutil mb -l REGION gs://BUCKET_NAME
```

---

## Performance Considerations

### For Large Tables (Millions of Rows)

1. **Run in Cloud Shell** for faster network to BigQuery/GCS
2. **Increase timeout** for Cloud Functions (max 3600 seconds)
3. **Process in batches** by date range if needed

### Memory Optimization

If processing very large datasets:
- Use Cloud Functions with 4GB or 8GB memory
- Consider processing one month at a time
- Implement pagination for extremely large daily volumes

### Cost Optimization

- **BigQuery:** Charged per bytes scanned (~$5 per TB)
- **Cloud Storage:** Charged for storage and egress
- **Cloud Functions:** Charged per invocation time and memory

**Estimate costs:**
```bash
# Check table size
bq show --format=prettyjson PROJECT:DATASET.TABLE | grep numBytes

# Estimate query cost (roughly)
Table_Size_TB * $5 = Estimated_Cost
```

---

## Examples

### Export Single Table (Default)

```python
# Configuration
TABLE_NAME = 'event_data_packet'
TIMESTAMP_COLUMN = 'generated_at'
```

### Export Multiple Tables

```python
TABLE_CONFIGS = [
    {
        'table_name': 'asset_telemetry_data_packet',
        'timestamp_column': 'time',
        'enabled': True
    },
    {
        'table_name': 'event_data_packet',
        'timestamp_column': 'generated_at',
        'enabled': True
    }
]
```

### Custom Date Range

```python
# Export from specific start date
START_DATE = datetime(2026, 1, 1, 0, 0, 0)
END_DATE = datetime(2026, 1, 31, 23, 59, 59)

START_DATE_UNIX = int(START_DATE.timestamp() * 1000)
END_DATE_UNIX = int(END_DATE.timestamp() * 1000)

# Update query
WHERE {TIMESTAMP_COLUMN} >= {START_DATE_UNIX} 
  AND {TIMESTAMP_COLUMN} <= {END_DATE_UNIX}
```

### Different Folder Structure

```python
# Current: 2026/02/02
date_key = export_date.strftime('%Y/%m/%d')

# Alternative formats:
date_key = export_date.strftime('%Y-%m-%d')     # 2026-02-02
date_key = export_date.strftime('%Y/%m')        # 2026/02
date_key = export_date.strftime('%Y/W%W')       # 2026/W05 (week)
```

---

## Project Structure

```
bigquery-gcs-export/
├── README.md                          # This file
├── standalone_export.py               # Standalone Python script
├── main_cloudfunction_fixed.py        # Cloud Function version
├── requirements.txt                   # Python dependencies (standalone)
├── requirements_cloudfunction.txt     # Python dependencies (Cloud Function)
├── .gitignore
└── docs/
    ├── DEPLOYMENT_GUIDE.md
    ├── TROUBLESHOOTING.md
    └── EXAMPLES.md
```


## Scheduling Automated Exports

### Using Cloud Scheduler (for Cloud Functions)

```bash
# Create Cloud Scheduler job (runs daily at 6 AM)
gcloud scheduler jobs create http bigquery-weekly-export \
  --schedule="0 6 * * *" \
  --uri="https://REGION-PROJECT.cloudfunctions.net/export-event-data" \
  --http-method=POST \
  --location=REGION
```

#   b i g q u e r y - t o - g c s - d a t a - b a c k f i l l 
 
 
