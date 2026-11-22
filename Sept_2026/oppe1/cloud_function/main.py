"""
OPPE 1 - Section C: Google Cloud Function (Gen 2)
Course: Introduction to Big Data

Triggered when the aggregated results CSV file is uploaded to the GCS bucket.
Reads the result file and prints the station with the highest average stop duration.
"""

import io
import csv
import functions_framework
from google.cloud import storage


@functions_framework.cloud_event
def process_station_results(cloud_event):
    """
    Cloud Function entry point.
    Triggered by GCS finalize event when the aggregated CSV is written or uploaded.
    """
    data = cloud_event.data
    bucket_name = data.get("bucket")
    file_name = data.get("name")

    print(f"Trigger received for file: gs://{bucket_name}/{file_name}")

    # Process only CSV files
    if not file_name or not file_name.endswith(".csv"):
        print(f"Skipping non-CSV file: {file_name}")
        return

    # Initialize GCS client and download text
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    content = blob.download_as_text(encoding="utf-8")
    csv_file = io.StringIO(content)
    reader = csv.DictReader(csv_file)

    max_avg_duration = -1.0
    highest_station = None
    total_stations_scanned = 0

    for row in reader:
        try:
            # Handle possible variations in column naming
            avg_str = row.get("avg_stop_duration") or row.get("avg_stop") or row.get("average_stop_duration")
            if avg_str is None:
                continue

            avg_val = float(avg_str)
            total_stations_scanned += 1

            if avg_val > max_avg_duration:
                max_avg_duration = avg_val
                highest_station = row
        except (ValueError, TypeError):
            continue

    # Format and log the final results to Cloud Logging
    print("\n" + "=" * 65)
    print("        OPPE 1 - STATION WITH HIGHEST AVERAGE STOP DURATION")
    print("=" * 65)
    print(f"Source File Analyzed:     gs://{bucket_name}/{file_name}")
    print(f"Total Stations Scanned:   {total_stations_scanned}")

    if highest_station:
        stn_code = highest_station.get("Station Code", "N/A")
        stn_name = highest_station.get("Station Name", "N/A")
        stoppages = highest_station.get("stoppage_count", "N/A")
        total_stop = highest_station.get("total_stop_duration", "N/A")

        print(f"Station Code:             {stn_code}")
        print(f"Station Name:             {stn_name}")
        print(f"Highest Avg Stop Duration:{max_avg_duration:.2f} minutes")
        print(f"Total Stoppages:          {stoppages}")
        print(f"Total Stop Duration:      {total_stop} minutes")
    else:
        print("No valid station records found in CSV.")

    print("=" * 65 + "\n")
