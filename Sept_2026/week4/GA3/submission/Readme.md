# GA3 — Click Counts by Time-of-Day Bins (PySpark on Dataproc)

## Assignment

Compute total click counts by time-of-day bins (0–6, 6–12, 12–18, 18–24) from a CSV file using PySpark on Google Cloud Dataproc. Implemented using both RDDs and DataFrames (no SparkSQL).

## Files

| File | Description |
|------|-------------|
| `ga3_click_counts.py` | PySpark script containing both RDD and DataFrame implementations. Takes an input CSV path and output GCS path as command-line arguments. |
| `output/rdd_output.txt` | Output from the **RDD-based** solution — each line is a `(time_bin, click_count)` tuple. |
| `output/df_output.csv` | Output from the **DataFrame-based** solution — a CSV with `time_bin` and `click_count` columns. |
| `Readme.md` | This file. |

## How to Run

```bash
gcloud dataproc jobs submit pyspark \
  gs://<BUCKET>/ga3_click_counts.py \
  --cluster=<CLUSTER_NAME> \
  --region=<REGION> \
  --project=<PROJECT_ID> \
  -- gs://<BUCKET>/input/click_file.txt gs://<BUCKET>/output
```

## Output Summary

| Time Bin | Click Count |
|----------|-------------|
| 0-6      | 44          |
| 6-12     | 53          |
| 12-18    | 48          |
| 18-24    | 55          |
| **Total**| **200**     |

