"""
Graded Assignment 1 - Week 2
Course: Introduction to Big Data

Task:
1. Download a text file from Google Cloud Storage (GCS) bucket.
2. Find the longest line(s) in the file based on character length.
   (If multiple lines share the maximum length, all are reported).
3. Print the results to the terminal.
4. Save the results to an output file and upload it back to the GCS bucket.
"""

import os
import argparse
from google.cloud import storage


def download_from_gcs(bucket_name: str, source_blob_name: str, destination_file_path: str):
    """Downloads a blob/file from the GCS bucket to a local path."""
    print(f"Connecting to GCS bucket '{bucket_name}'...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    print(f"Downloading 'gs://{bucket_name}/{source_blob_name}' to '{destination_file_path}'...")
    blob.download_to_filename(destination_file_path)
    print("Download completed successfully.\n")


def find_longest_lines(file_path: str):
    """
    Reads the file and identifies the line(s) with the maximum character length.
    Returns:
        max_length (int): The length of the longest line(s).
        longest_lines (list of tuple): List of (line_number, line_text).
        total_lines (int): Total number of lines processed.
    """
    longest_lines = []
    max_length = 0
    total_lines = 0

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line_num, line in enumerate(f, start=1):
            total_lines += 1
            # Remove trailing newline characters for accurate character length comparison
            clean_line = line.rstrip("\r\n")
            line_len = len(clean_line)

            if line_len > max_length:
                max_length = line_len
                longest_lines = [(line_num, clean_line)]
            elif line_len == max_length and line_len > 0:
                longest_lines.append((line_num, clean_line))

    return max_length, longest_lines, total_lines


def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str):
    """Uploads a local file to the GCS bucket."""
    print(f"\nUploading '{source_file_path}' to 'gs://{bucket_name}/{destination_blob_name}'...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_path)
    print(f"Output successfully saved to 'gs://{bucket_name}/{destination_blob_name}'.")


def main():
    parser = argparse.ArgumentParser(description="Find the longest line(s) in a file from GCS.")
    parser.add_argument("--bucket", default="ibd_bucket_gokul", help="GCS bucket name")
    parser.add_argument("--input-blob", default="wk_1_input.txt", help="Input file name in GCS")
    parser.add_argument("--output-blob", default="output.txt", help="Output file name in GCS")
    args = parser.parse_args()

    bucket_name = args.bucket
    input_blob_name = args.input_blob
    output_blob_name = args.output_blob

    # Local file paths (stored in the same directory as this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_input_path = os.path.join(script_dir, "downloaded_input.txt")
    local_output_path = os.path.join(script_dir, "output.txt")

    # Step 1: Download the file from GCS
    download_from_gcs(bucket_name, input_blob_name, local_input_path)

    # Step 2: Find the longest line(s)
    max_length, longest_lines, total_lines = find_longest_lines(local_input_path)

    # Step 3: Format the results
    result_lines = []
    result_lines.append("=" * 60)
    result_lines.append("              LONGEST LINE ANALYSIS RESULTS")
    result_lines.append("=" * 60)
    result_lines.append(f"Source Bucket:       gs://{bucket_name}/{input_blob_name}")
    result_lines.append(f"Total Lines Scanned: {total_lines}")
    result_lines.append(f"Maximum Line Length: {max_length} characters")
    result_lines.append(f"Number of Longest Lines: {len(longest_lines)}")
    result_lines.append("-" * 60)

    for idx, (line_num, line_text) in enumerate(longest_lines, start=1):
        result_lines.append(f"Match #{idx} (Line {line_num}, {len(line_text)} chars):")
        result_lines.append(f"  {line_text}")
        result_lines.append("-" * 60)

    formatted_output = "\n".join(result_lines)

    # Print to Terminal
    print(formatted_output)

    # Step 4: Save output to a local file
    with open(local_output_path, "w", encoding="utf-8") as out_file:
        out_file.write(formatted_output + "\n")
    print(f"\nLocal results written to '{local_output_path}'.")

    # Step 5: Upload output file back to GCS bucket
    upload_to_gcs(bucket_name, local_output_path, output_blob_name)
    print("\nAll tasks completed successfully!")


if __name__ == "__main__":
    main()
