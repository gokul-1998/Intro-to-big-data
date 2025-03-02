from google.cloud import storage

def get_line_count_from_gcs(bucket_name, file_name):
    # Create a Cloud Storage client
    gcs_client = storage.Client()

    # Access the desired bucket
    bucket_obj = gcs_client.get_bucket(bucket_name)

    # Retrieve the specific file (blob)
    file_blob = bucket_obj.get_blob(file_name)

    # Download the file content as bytes
    file_data = file_blob.download_as_string()

    # Calculate the number of lines by counting newline characters
    total_lines = file_data.count(b'\n') + 1

    return total_lines

def cl_gcs(event,context):
    # Set the bucket and file name variables
    cloud_bucket_name = event['bucket']
    gcs_file_name = event['name']

    # Call the function to get the line count
    lines_in_file = get_line_count_from_gcs(cloud_bucket_name, gcs_file_name)

    # Output the line count
    print(f'The file "{gcs_file_name}" in bucket "{cloud_bucket_name}" has {lines_in_file} lines.')