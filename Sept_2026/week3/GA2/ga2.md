## Graded Assignment 2 Week 3
- You are required to use Google Cloud Platform (GCP) to `analyze a text file stored in Google Cloud Storage (GCS)`. 
- The task is to `identify the longest line(s) in the file`. If multiple lines share the maximum length, all of them should be reported.

- Write a `Google Cloud Function that is triggered whenever a file is uploaded to a GCS bucket`.

The function should:

- Download the uploaded file.
- Find the longest line(s) in the file (based on character length).
- Print the results in Cloud Logging.
- Save the results back into the same GCS bucket

### Submission Requirements:

    - Python code for the Cloud Function.
    - The result file downloaded from the GCS bucket (output file).
    - Screenshot of the Logs Explorer showing the printed results.
    - Important Note for All Assignments

- All assignments must be implemented and executed on Google Cloud Platform (GCP) using the specified services Local execution will not be accepted.
- Along with the code and output files, students must submit a screencast video (5–10 minutes) for each assignment. The video should cover:
- Explanation of the code logic.
- Pipeline/architecture explanation if applicable.
- Live demonstration of execution and the outputs/results.
- Resource setup in GCP
- Screencasts must be clear enough to verify that the work was completed by the student on GCP.
- Videos without audio will not be considered for evaluation Single Submission

- Only one submission is allowed. Once submitted, it cannot be edited.
