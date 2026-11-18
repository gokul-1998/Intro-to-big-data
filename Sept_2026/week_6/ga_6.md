## Problem Statement
-  Use `Google Cloud Dataproc` and `SparkSQL` to 
    - analyze customer data and
    -  demonstrate the handling of Slowly Changing Dimensions (SCD).

- All data is provided to you through Google Drive links. Download the files and upload them to your Google Cloud Storage (GCS) bucket before starting the analysis.

Dataset Link : https://drive.google.com/drive/folders/1d1QPXN0Vi9XEaq74g16rUXAylKQjfNIf

- Aggregation Queries : Using the transactions data, write SparkSQL queries to compute:
    - Top 5 customers by total transaction amount
    - Number of customers per city
    - Average transaction value overall

- Save each query result as a separate CSV in GCS.
    - Slowly Changing Dimensions (SCD)Use the Customer Master and Customer Updates datasets to demonstrate:
        - SCD Type I
        - SCD Type II
    - Show before and after states of the Customer Master table for both SCD Type I and Type II.
    - Output and ValidationPrint or log query outputs and table previews in the driver logs.
    - Ensure all updated tables or query results are written back to GCS.

### Submission Requirements
    - SparkSQL code file (.py containing SQL statements)
    - Output files downloaded from GCS
    - Screenshot of Dataproc job details page showing successful execution and log output
    - Screencast demonstrating:
        - GCP setup
        - SQL logic explanation
        -   Live run of queries
### Note

- Use only SparkSQL (no PySpark DataFrame API).
- All tasks must be executed on GCP Cluster using GCS for input and output.
- Submissions past the deadline will not be evaluated.