from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

def bq_daily_export(request):

    print("=" * 70)
    print("DAILY BIGQUERY EXPORT (STRICT IST - NO GAPS)")
    print("=" * 70)

    bq_client = bigquery.Client()
    IST = pytz.timezone("Asia/Kolkata")

    project_id = "gcp-poc-cloudisde"
    dataset_id = "prod_iot"
    bucket_name = "datoms-poc-bkt-bq-daily-live-data-backup-csv"

    # ---------------------------------------------------
    # Always export PREVIOUS IST calendar day
    # ---------------------------------------------------
    now_ist = datetime.now(IST)
    target_date = (now_ist - timedelta(days=1)).date()

    # Build IST midnight
    day_start_ist = IST.localize(
        datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    )

    # Build next day's IST midnight (exclusive boundary)
    next_day_ist = day_start_ist + timedelta(days=1)

    # Convert IST → UTC → UNIX
    start_unix = int(day_start_ist.astimezone(pytz.utc).timestamp())
    next_day_unix = int(next_day_ist.astimezone(pytz.utc).timestamp())

    print(f"Exporting IST Date: {target_date}")
    print(f"IST Window: {day_start_ist} → {next_day_ist} (exclusive)")
    print(f"UNIX Range: {start_unix} → {next_day_unix}")

    tables = [
        {
            "table": "asset_telemetry_data_packet",
            "timestamp_column": "time",
            "select_clause": """
                asset_id,
                asset_category_id,
                time,
                server_time,
                data_is_historical,
                ingestion_time,
                TO_JSON_STRING(data) AS data
            """
        },
        {
            "table": "event_data_packet",
            "timestamp_column": "generated_at",
            "select_clause": """
                generated_by,
                rule_id,
                service_id,
                client_id,
                source_type,
                asset_id,
                message,
                message_template_id,
                TO_JSON_STRING(details) AS details,
                TO_JSON_STRING(actions) AS actions,
                TO_JSON_STRING(rule_tags) AS rule_tags,
                violation_type,
                should_event_be_visible,
                ingestion_time,
                event_id,
                generated_at
            """
        }
    ]

    for cfg in tables:

        table_name = cfg["table"]
        timestamp_column = cfg["timestamp_column"]
        source_table = f"{project_id}.{dataset_id}.{table_name}"

        folder_path = day_start_ist.strftime("%Y/%m/%d")
        destination_uri = (
            f"gs://{bucket_name}/{table_name}/{folder_path}/"
            f"{table_name}-{day_start_ist:%Y%m%d}-*.csv"
        )

        print("\n" + "-" * 60)
        print(f"Processing {table_name}")

        # Exact same filter used for both count & export
        filter_condition = f"""
        {timestamp_column} >= {start_unix}
        AND {timestamp_column} < {next_day_unix}
        """

        # Count check
        count_query = f"""
        SELECT COUNT(*) AS row_count
        FROM `{source_table}`
        WHERE {filter_condition}
        """

        count_job = bq_client.query(count_query, location="asia-south1")
        row_count = list(count_job.result())[0].row_count

        print(f"Rows Found: {row_count}")

        if row_count == 0:
            print("No data found, skipping.")
            continue

        # Export
        export_query = f"""
        EXPORT DATA OPTIONS(
            uri='{destination_uri}',
            format='CSV',
            overwrite=true,
            header=true
        ) AS
        SELECT {cfg["select_clause"]}
        FROM `{source_table}`
        WHERE {filter_condition}
        """

        job = bq_client.query(export_query, location="asia-south1")
        job.result()

        print(f"✓ Export complete for {table_name}")

    print("\n✓ DAILY IST EXPORT FINISHED")
    return "Daily export completed"