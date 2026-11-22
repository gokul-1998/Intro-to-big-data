
### Step 1: Clean Up Old Output Files from GCS

```bash
gcloud storage rm --recursive gs://gokul_ibd_bucket/output/station_summary
gcloud storage rm gs://gokul_ibd_bucket/station_summary_final.csv
```

### Step 2: Submit the PySpark Job to Dataproc
Run the PySpark analysis job on your Dataproc cluster in `asia-south1`:

```bash
gcloud dataproc jobs submit pyspark gs://gokul_ibd_bucket/scripts/oppe1_pyspark.py \
  --cluster=gokul-s-dataproc-cluster \
  --region=asia-south1 \
  --project=project-c38127c2-5130-4284-af3 \
  -- \
  gs://gokul_ibd_bucket/input/Train_details_22122017.csv \
  gs://gokul_ibd_bucket/output/station_summary
```
> - *"The PySpark job reads the dataset, cleans malformed rows where SEQ is non-numeric, and discards placeholder 00:00:00 rows."*
> - *"It calculates stop durations in minutes, accounting for midnight crossovers where arrival is before midnight and departure is after."*
> - *"Section A outputs the Top 5 stations by stoppages: MSB, SDAH, HWH, KYN, DR."*
> - *"Section B identifies 621 stations where max stop > 3x average stop, and calculates a maximum of 112 trains in any 2-hour sliding window for Chennai Beach (MSB)."*
> - *"Section C coalesces the aggregated results into a single CSV in GCS."*

---

### Step 3: Verify the Output CSV in GCS
Show the evaluator that the PySpark job successfully wrote the output CSV:

```bash
gcloud storage ls gs://gokul_ibd_bucket/output/station_summary/
```
*"We can see the PySpark job has written the aggregated station summary CSV into our GCS output folder."*

---

### Step 4: Trigger the Cloud Function
Copy the generated `part-*.csv` file to trigger your Cloud Function:

```bash
gcloud storage cp gs://gokul_ibd_bucket/output/station_summary/*.csv gs://gokul_ibd_bucket/station_summary_final.csv
```
>  *"Now, copying this CSV to `station_summary_final.csv` emits an Object Finalize event that automatically triggers our Google Cloud Function."*

---

### Step 5: View the Cloud Function Logs
Wait ~10–15 seconds for the trigger, then display the Cloud Function execution log in your terminal:

```bash
gcloud functions logs read process_station_results --gen2 --region=us-east1 --limit 30
```

*"In the Cloud Function logs, we can see the function read the CSV in-memory and printed the station with the highest average stop duration: **MANDOR (MDB)** with **47.20 minutes**."*