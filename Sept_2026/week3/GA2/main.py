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
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    if file_name.startswith("output_"):
        return

    # Download from GCS
    storage_client = storage.Client()
    blob = storage_client.bucket(bucket_name).blob(file_name)
    content = blob.download_as_text(encoding="utf-8")

    # Find longest lines and format
    max_length, longest_lines, total_lines = find_longest_lines(content)
    formatted_output = format_results(
        bucket_name, file_name, max_length, longest_lines, total_lines
    )

    # Print to Cloud Logging
    print(formatted_output)

    # Save output_<filename> to the same bucket
    out_blob = storage_client.bucket(bucket_name).blob(f"output_{file_name}")
    out_blob.upload_from_string(formatted_output, content_type="text/plain")
    print(f"Results saved to gs://{bucket_name}/output_{file_name}")
