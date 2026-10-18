import os
import argparse
from google.cloud import storage


def upload_to_gcs(bucket_name: str, source_file_path: str, destination_blob_name: str = None):
    """Uploads a local file to a Google Cloud Storage bucket.

    Args:
        bucket_name (str): The name of your GCS bucket.
        source_file_path (str): The path to the local file to upload.
        destination_blob_name (str, optional): The destination object name in GCS.
            Defaults to the filename of source_file_path.
    """
    if destination_blob_name is None:
        destination_blob_name = os.path.basename(source_file_path)

    # Initialize GCS client
    # Note: Requires GOOGLE_APPLICATION_CREDENTIALS environment variable or active GCP VM/gcloud auth
    storage_client = storage.Client()

    # Get the target bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object pointing to the desired destination
    blob = bucket.blob(destination_blob_name)

    # Upload the file
    print(f"Uploading '{source_file_path}' to 'gs://{bucket_name}/{destination_blob_name}'...")
    blob.upload_from_filename(source_file_path)

    print(f"Successfully uploaded '{source_file_path}' to 'gs://{bucket_name}/{destination_blob_name}'.")


def main():
    # parser = argparse.ArgumentParser(description="Upload a file to a Google Cloud Storage bucket.")
    # parser.add_argument("--bucket", default="your-bucket-name", help="Name of the GCS bucket")
    # parser.add_argument("--source", default="input.txt", help="Path to local file to upload")
    # parser.add_argument("--destination", default=None, help="Destination blob name in GCS (optional)")

    # args = parser.parse_args()

    # You can customize these defaults or pass them via CLI flags:
    # python 1.py --bucket <bucket_name> --source <file_path> [--destination <blob_name>]
    upload_to_gcs(
        bucket_name="ibd_bucket_gokul",
        source_file_path=r"Sept_2026\week2\GA1\input.txt",
        destination_blob_name="wk_1_input.txt"
    )


if __name__ == "__main__":
    main()