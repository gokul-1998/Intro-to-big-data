Instructions
Problem Statement
The Railway Board wants to analyze train stoppage patterns and platform utilization at railway stations using arrival and departure data.

Using PySpark on Google Cloud Platform (GCP), perform the following analysis.

Instructions
Use GCP services (Cluster and Cloud Storage / GCS) for computation and storage.
Handle missing values and malformed rows appropriately.
Do not assume the dataset is sorted.
Demonstrate the exact PySpark functions used for computations.
Data - https://drive.google.com/file/d/1e5LdwV-LD9240EQqxc0OedYbBfuaF7FZ/view
Section A – Stop Duration Statistics
Compute the stop duration (in minutes) for each train at each station.
For each station, compute:
Minimum stop duration
Maximum stop duration
Average stop duration
90th percentile stop duration
Identify the top 5 stations with the highest number of train stoppages.
Section B – Station Load Analysis
For each station, compute:
Total stop duration
Average stop duration
Identify stations where: max stop duration > 3 × average stop duration
For any one of the top 5 stations, compute the maximum number of trains arriving within any 2-hour sliding window.
Section C – Automation
Store the aggregated results as a CSV file in GCS.
Write a Google Cloud Function that:
Triggers when the CSV file is uploaded to the bucket
Reads the result file
Prints the station with the highest average stop duration
Deliverables
Submit:

PySpark code and GCF code (.py files).
Screenshot of Cloud Function logs.
Short screen recording showing live execution, outputs, code explanations, and GCP setup.
IBD OPPE LLM Reference Documentation (Click here for the template)
Submission form
Notes
Videos without voice will not be considered for evaluation.
Submissions past the deadline will not be evaluated.
Collaboration is not allowed. This is an open-internet individual exam.