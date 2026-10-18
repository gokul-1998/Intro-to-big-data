- 
    ```
    sudo apt install git
    ```
-   ```
    sudo apt update
    ```

- this is needed to authenticate the gcloud.
    ```
    gcloud auth application-default login --no-launch-browser
    ```

- Download `output.txt` from `gs://ibd_bucket_gokul/output.txt` via GCP Console or CLI:
    ```
    gsutil cp gs://ibd_bucket_gokul/output.txt .
    ```