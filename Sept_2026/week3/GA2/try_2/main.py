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
    max_len, longest_lines, total_lines = find_longest_lines(content)
    formatted_output = format_results(...)

    # Print to Cloud Logging
    print(formatted_output)

    # Save output_<filename> to the same bucket
    out_blob = storage_client.bucket(bucket_name).blob(f"output_{file_name}")
    out_blob.upload_from_string(formatted_output, content_type="text/plain")