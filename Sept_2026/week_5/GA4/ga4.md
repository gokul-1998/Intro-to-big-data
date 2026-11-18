Week 5 Graded Assignment 4
Submission form

Problem Statement
- Use Google Cloud Dataproc and the PySpark DataFrame API to perform a series of data transformations, joins, and aggregations on small datasets stored in Google Cloud Storage (GCS).

Two datasets will be provided to you through Google Drive links:

Customer Dataset– containing columns such as customer_id, customer_name, city, status
Transaction Dataset – containing columns such as transaction_id, customer_id, transaction_date, transaction_amount, payment_mode
Upload both files to your GCS bucket before starting the analysis.

Write a PySpark DataFrame program that performs the following operations on a Dataproc cluster using GCS as input and output locations:

1. Load and Inspect
Load both datasets from GCS and inspect their schemas and sample rows.
2. Column Transformations
Create a conditional column based on transaction_amount (e.g., Low, Medium, High).
Extract the month from transaction_date.
Standardize string values where required (for example, city names).
3. Data Cleaning
Identify, clean, and remove invalid or duplicate records before proceeding to joins or aggregations.

Definitions of Invalid Records:

A record is considered invalid if it meets any of the following conditions:

Missing or blank values in key fields such as customer_id, transaction_id, city, or status.
Negative or zero values in transaction_amount.
Invalid or malformed transaction_date.
Inconsistent or null entries in other mandatory columns.
Nature of Cleaning to Perform:

Remove or isolate invalid records:
Extract all invalid rows and save them separately as invalid_rows.
Save the cleaned dataset (after removing invalid and duplicate records) as cleaned_data.
Handle duplicate records:
Treat records with identical values across all columns as duplicates.
Retain only one instance and mark others as invalid.
Standardize city names :
Correct inconsistent spellings and capitalization (e.g., “kochi”, “KOCHI”, “Cochin” → “Kochi”).
Apply consistent formatting, such as title case, to all city values.
Verify data formats:
Ensure all dates are in YYYY-MM-DD format.
Confirm that numeric fields (e.g., transaction_amount) are stored with proper data types.
Ensure referential integrity:
Every customer_id appearing in the transaction dataset must exist in the customer dataset.
The cleaning process should be carried out separately for both datasets before performing any joins.

Log a few rows of invalid and cleaned records in your driver output for verification.

4. Joins
Join the Customer and Transaction DataFrames on customer_id.
Save the joined results as joined_data.
5. Aggregations
From the cleaned and joined data, compute and save the following:

Total and average transaction amount per customer.
Total transaction amount per city.
Top 3 customers by total transaction amount.
Save these results as aggregates.

6. Logging and Outputs
Print a few rows from each major stage in your Dataproc driver logs.

Ensure the following outputs are created in your GCS bucket: cleaned_data, invalid_rows, invalid_rows, joined_data, aggregates

Submission Requirements
PySpark code file (.py)
All output files downloaded from GCS bucket
Screenshot of the Dataproc job details page showing successful execution and log output
Screencast demonstrating GCP setup, code logic, and live run
Note
Use only the PySpark DataFrame API (no SparkSQL).
All tasks must be executed on GCP Dataproc using GCS for input/output.
Submissions past the deadline will not be evaluated.