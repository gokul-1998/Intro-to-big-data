"""
Graded Assignment 2 - Week 3
Course: Introduction to Big Data

Google Cloud Function triggered by GCS file upload.
- Downloads the uploaded file.
- Finds the longest line(s) based on character length.
- Prints results to Cloud Logging.
- Saves results back to the same GCS bucket.
"""

import functions_framework
from google.cloud import storage


def find_longest_lines(content: str):
    """
    Analyzes text content and identifies the line(s) with the maximum character length.
    Returns:
        max_length (int): The length of the longest line(s).
        longest_lines (list of tuple): List of (line_number, line_text).
        total_lines (int): Total number of lines processed.
    """
    longest_lines = []
    max_length = 0
    total_lines = 0

    for line_num, line in enumerate(content.splitlines(), start=1):
        total_lines += 1
        # Remove trailing whitespace for accurate length comparison
        clean_line = line.rstrip("\r\n")
        line_len = len(clean_line)

        if line_len > max_length:
            max_length = line_len
            longest_lines = [(line_num, clean_line)]
        elif line_len == max_length and line_len > 0:
            longest_lines.append((line_num, clean_line))

    return max_length, longest_lines, total_lines


def format_results(bucket_name, file_name, max_length, longest_lines, total_lines):
    """Formats the analysis results into a readable string."""
    result_lines = []
    result_lines.append("=" * 60)
    result_lines.append("              LONGEST LINE ANALYSIS RESULTS")
    result_lines.append("=" * 60)
    result_lines.append(f"Source File:         gs://{bucket_name}/{file_name}")
    result_lines.append(f"Total Lines Scanned: {total_lines}")
    result_lines.append(f"Maximum Line Length:  {max_length} characters")
    result_lines.append(f"Number of Longest Lines: {len(longest_lines)}")
    result_lines.append("-" * 60)

    for idx, (line_num, line_text) in enumerate(longest_lines, start=1):
        result_lines.append(f"Match #{idx} (Line {line_num}, {len(line_text)} chars):")
        result_lines.append(f"  {line_text}")
        result_lines.append("-" * 60)

    return "\n".join(result_lines)


@functions_framework.cloud_event
def process_file(cloud_event):
    """
    Cloud Function entry point.
    Triggered by a file upload (finalize event) in the GCS bucket.
    """
    data = cloud_event.data

    bucket_name = data["bucket"]
    file_name = data["name"]

    # Avoid infinite loop: skip processing our own output file
    if file_name.startswith("output_"):
        print(f"Skipping output file: {file_name}")
        return

    print(f"Processing file: gs://{bucket_name}/{file_name}")

    # Step 1: Download the uploaded file content
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text(encoding="utf-8")

    # Step 2: Find the longest line(s)
    max_length, longest_lines, total_lines = find_longest_lines(content)

    # Step 3: Format and print results to Cloud Logging
    formatted_output = format_results(
        bucket_name, file_name, max_length, longest_lines, total_lines
    )
    print(formatted_output)

    # Step 4: Save results back to the same GCS bucket
    output_blob_name = f"output_{file_name}"
    output_blob = bucket.blob(output_blob_name)
    output_blob.upload_from_string(formatted_output, content_type="text/plain")

    print(f"Results saved to gs://{bucket_name}/{output_blob_name}")
