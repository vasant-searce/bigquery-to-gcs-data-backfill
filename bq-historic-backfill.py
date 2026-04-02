from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz

def bq_historic_backfill(request):

    print("=" * 70)
    print("HISTORIC BIGQUERY EXPORT TO GCS")
    print("=" * 70)

    bq_client = bigquery.Client()
    IST = pytz.timezone("Asia/Kolkata")

    project_id = "gcp-poc-cloudisde"
    dataset_id = "prod_iot"
    bucket_name = "datoms-poc-bkt-bq-daily-live-data-backup-csv"

    # -----------------------------
    # TELEMETRY CONFIG
    # -----------------------------
    telemetry_start_unix = 1770099859

    # -----------------------------
    # EVENT CONFIG
    # -----------------------------
    event_start_unix = 1770232367

    # -----------------------------
    # END DATE = 26 FEB IST
    # -----------------------------
    end_date_ist = IST.localize(datetime(2026, 2, 26, 23, 59, 59))
    end_unix = int(end_date_ist.astimezone(pytz.utc).timestamp())

    tables = [
        {
            "table": "asset_telemetry_data_packet",
            "timestamp_column": "time",
            "start_unix": telemetry_start_unix,
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
            "start_unix": event_start_unix,
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

        print("\n" + "=" * 60)
        print(f"Processing {cfg['table']}")
        print("=" * 60)

        current_start_unix = cfg["start_unix"]

        while current_start_unix <= end_unix:

            # Convert start unix → IST date
            start_dt_utc = datetime.utcfromtimestamp(current_start_unix).replace(tzinfo=pytz.utc)
            start_dt_ist = start_dt_utc.astimezone(IST)

            day_start_ist = IST.localize(
                datetime(start_dt_ist.year, start_dt_ist.month, start_dt_ist.day, 0, 0, 0)
            )
            day_end_ist = IST.localize(
                datetime(start_dt_ist.year, start_dt_ist.month, start_dt_ist.day, 23, 59, 59)
            )

            day_start_unix = int(day_start_ist.astimezone(pytz.utc).timestamp())
            day_end_unix = int(day_end_ist.astimezone(pytz.utc).timestamp())

            if day_end_unix > end_unix:
                day_end_unix = end_unix

            folder_path = day_start_ist.strftime("%Y/%m/%d")

            destination_uri = (
                f"gs://{bucket_name}/{cfg['table']}/{folder_path}/"
                f"{cfg['table']}-{day_start_ist:%Y%m%d}-*.csv"
            )

            print(f"\nExporting {folder_path}")
            print(f"UNIX Range: {day_start_unix} → {day_end_unix}")

            source_table = f"{project_id}.{dataset_id}.{cfg['table']}"

            count_query = f"""
            SELECT COUNT(*) AS row_count
            FROM `{source_table}`
            WHERE {cfg['timestamp_column']} 
            BETWEEN {day_start_unix} AND {day_end_unix}
            """

            count_job = bq_client.query(count_query, location="asia-south1")
            row_count = list(count_job.result())[0].row_count

            if row_count == 0:
                print("No rows found, skipping.")
            else:
                print(f"Rows Found: {row_count}")

                export_query = f"""
                EXPORT DATA OPTIONS(
                    uri='{destination_uri}',
                    format='CSV',
                    overwrite=true,
                    header=true
                ) AS
                SELECT {cfg['select_clause']}
                FROM `{source_table}`
                WHERE {cfg['timestamp_column']}
                BETWEEN {day_start_unix} AND {day_end_unix}
                """

                job = bq_client.query(export_query, location="asia-south1")
                job.result()

                print("✓ Export complete")

            # Move to next day
            next_day_ist = day_start_ist + timedelta(days=1)
            current_start_unix = int(next_day_ist.astimezone(pytz.utc).timestamp())

    print("\n✓ HISTORIC BACKFILL COMPLETE")
    return "Historic export completed"