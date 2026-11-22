#!/bin/bash
# ==============================================================================
# OPPE 1 - Live Demonstration Script for Screen Recording
# Course: Introduction to Big Data
# ==============================================================================

set -e

PROJECT_ID="project-c38127c2-5130-4284-af3"
DATAPROC_REGION="asia-south1"
CLUSTER_NAME="gokul-s-dataproc-cluster"
BUCKET_NAME="gokul_ibd_bucket"
CF_REGION="us-east1"

echo "=================================================================="
echo "          OPPE 1: LIVE DEMONSTRATION & EXECUTION"
echo "=================================================================="
echo "Project ID:        ${PROJECT_ID}"
echo "Dataproc Cluster:  ${CLUSTER_NAME} (${DATAPROC_REGION})"
echo "Storage Bucket:    gs://${BUCKET_NAME}"
echo "=================================================================="

# ------------------------------------------------------------------------------
# STEP 1: CLEAN UP OLD OUTPUT FILES IN GCS
# ------------------------------------------------------------------------------
echo ""
echo "[Step 1] Cleaning up previous output artifacts from GCS..."
gcloud storage rm --recursive "gs://${BUCKET_NAME}/output/station_summary" 2>/dev/null || true
gcloud storage rm "gs://${BUCKET_NAME}/station_summary_final.csv" 2>/dev/null || true
echo "Cleanup complete. Fresh run environment ready."

# ------------------------------------------------------------------------------
# STEP 2: SUBMIT PYSPARK JOB TO DATAPROC
# ------------------------------------------------------------------------------
echo ""
echo "[Step 2] Submitting PySpark Job to Dataproc Cluster..."
gcloud dataproc jobs submit pyspark gs://${BUCKET_NAME}/scripts/oppe1_pyspark.py \
  --cluster=${CLUSTER_NAME} \
  --region=${DATAPROC_REGION} \
  --project=${PROJECT_ID} \
  -- \
  gs://${BUCKET_NAME}/input/Train_details_22122017.csv \
  gs://${BUCKET_NAME}/output/station_summary

# ------------------------------------------------------------------------------
# STEP 3: TRIGGER CLOUD FUNCTION WITH THE GENERATED CSV
# ------------------------------------------------------------------------------
echo ""
echo "[Step 3] Triggering Cloud Function with the generated PySpark summary..."
# Locate the generated part-*.csv file in the output folder
PART_FILE=$(gcloud storage ls "gs://${BUCKET_NAME}/output/station_summary/*.csv" | head -n 1)
echo "Generated CSV file: ${PART_FILE}"

# Copy to trigger the Cloud Function
gcloud storage cp "${PART_FILE}" "gs://${BUCKET_NAME}/station_summary_final.csv"

# ------------------------------------------------------------------------------
# STEP 4: FETCH AND DISPLAY CLOUD FUNCTION EXECUTION LOGS
# ------------------------------------------------------------------------------
echo ""
echo "[Step 4] Waiting 15 seconds for Cloud Function execution..."
sleep 15

echo ""
echo "=================================================================="
echo "                 CLOUD FUNCTION EXECUTION LOGS"
echo "=================================================================="
gcloud functions logs read process_station_results --gen2 --region=${CF_REGION} --limit 30

echo ""
echo "=================================================================="
echo "          LIVE DEMONSTRATION COMPLETED SUCCESSFULLY"
echo "=================================================================="
