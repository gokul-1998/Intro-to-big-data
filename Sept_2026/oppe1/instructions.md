# OPPE 1 - Train Stoppage Patterns & Platform Utilization

## 1. Overview
This project contains the complete solution for **OPPE 1 (Introduction to Big Data)**:
- **PySpark on Dataproc:** Data cleaning, stop duration computations, 90th percentile, station load analysis, 2-hour sliding window, and aggregated CSV export.
- **Google Cloud Function:** Event-driven serverless automation that triggers on CSV upload, reads the aggregated station metrics, and logs the station with the highest average stop duration.

---

## 2. Solution Summary & Verified Values

From the dataset `Train_details_22122017.csv`:

### Section A: Stop Duration Statistics
| Station Code | Station Name | Stoppages | Min (min) | Max (min) | Avg (min) | 90th %ile (min) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **MSB** | CHENNAI BEACH | 735 | 0.0 | 25.0 | 0.45 | 0.0 |
| **SDAH** | SEALDAH | 729 | 0.0 | 0.0 | 0.00 | 0.0 |
| **HWH** | HOWRAH JN. | 676 | 0.0 | 50.0 | 0.85 | 0.0 |
| **KYN** | KALYAN JN | 611 | 0.0 | 5.0 | 1.28 | 3.0 |
| **DR** | DADAR | 507 | 0.0 | 5.0 | 1.28 | 3.0 |

### Section B: Station Load Analysis
- **Stations with `max_stop > 3 × avg_stop`:** **621 stations**.
- **2-Hour Sliding Window Peak Arrival (for top station MSB):** **112 trains**.

### Section C: Cloud Function Automation
- **Station with Highest Average Stop Duration:** **`MDB` (MANDOR)** with an average stop duration of **47.20 minutes** (total 5 stoppages, max 230 minutes).

---

## 3. Step-by-Step GCP Execution Guide

### Step 1: Upload Dataset & PySpark Script to GCS
In Google Cloud Shell:

```bash
# Set your active project and bucket
PROJECT_ID="project-c38127c2-5130-4284-af3"
BUCKET_NAME="gokul_ibd_bucket"

# Upload dataset to GCS
gcloud storage cp Train_details_22122017.csv gs://${BUCKET_NAME}/input/Train_details_22122017.csv

# Upload PySpark script to GCS
gcloud storage cp oppe1_pyspark.py gs://${BUCKET_NAME}/scripts/oppe1_pyspark.py
```

---

### Step 2: Create / Verify Dataproc Cluster in asia-south1
If you do not have an active Dataproc cluster, create a single-node cluster in `asia-south1`:

```bash
gcloud dataproc clusters create gokul-s-dataproc-cluster \
  --project=project-c38127c2-5130-4284-af3 \
  --region=asia-south1 \
  --single-node \
  --master-machine-type=e2-standard-2 \
  --master-boot-disk-size=50GB \
  --max-idle=30m
```

---

### Step 3: Submit the PySpark Job on Dataproc (asia-south1)
Submit the PySpark job:

```bash
gcloud dataproc jobs submit pyspark gs://gokul_ibd_bucket/scripts/oppe1_pyspark.py \
  --cluster=gokul-s-dataproc-cluster \
  --region=asia-south1 \
  --project=project-c38127c2-5130-4284-af3 \
  -- \
  gs://gokul_ibd_bucket/input/Train_details_22122017.csv \
  gs://gokul_ibd_bucket/output/station_summary
```

During execution, the Dataproc job will output the formatted tables for Section A and Section B, and save the aggregated CSV to `gs://${BUCKET_NAME}/output/station_summary/`.

---

### Step 4: Deploy the Cloud Function (Section C)
Navigate into the `cloud_function` directory and deploy the Cloud Function:

```bash
cd cloud_function

gcloud functions deploy process_station_results \
  --gen2 \
  --runtime python312 \
  --region us-east1 \
  --source . \
  --entry-point process_station_results \
  --trigger-location us-east1 \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=${BUCKET_NAME}" \
  --allow-unauthenticated
```

---

### Step 5: Test & View Cloud Function Execution Logs
Trigger the function with the generated CSV from Dataproc:

```bash
# Find the generated CSV file
gcloud storage ls gs://${BUCKET_NAME}/output/station_summary/*.csv

# Copy the CSV to trigger the Cloud Function
gcloud storage cp gs://${BUCKET_NAME}/output/station_summary/*.csv gs://${BUCKET_NAME}/station_summary_final.csv

# Wait ~20 seconds, then check logs
gcloud functions logs read process_station_results --gen2 --region us-east1 --limit 50
```

---

## 4. Video Screencast Guide (5–10 Minutes)
1. **Introduction & Architecture (1–2 min):**
   - Explain the goal of the Railway Board analysis.
   - Mention the GCP pipeline: GCS (storage) → Dataproc (PySpark computations) → GCS (aggregated CSV) → Cloud Function (automation) → Cloud Logging.
2. **Code Walkthrough (2–3 min):**
   - Walk through `oppe1_pyspark.py`: malformed row filtering, stop duration formula with midnight crossover (`when(diff < 0, diff + 86400)`), Section A percentile & aggregation, Section B sliding window using `Window.orderBy("arr_sec").rangeBetween(0, 7200)`.
   - Walk through `cloud_function/main.py`: reading CSV in-memory and finding the highest average stop duration.
3. **Live Demonstration (2–3 min):**
   - Show the Dataproc job running or finished with results in the job output.
   - Show the output file in GCS.
   - Show the Cloud Function execution log in Logs Explorer highlighting `MDB (MANDOR)` with 47.20 minutes.
