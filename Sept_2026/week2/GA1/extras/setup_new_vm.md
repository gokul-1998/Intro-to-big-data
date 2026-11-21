- 
    ```
    sudo apt install git
    sudo apt update
    ```

- this is needed to authenticate the gcloud.
    ```
    gcloud auth application-default login --no-launch-browser
    ```

- Download `output.txt` from `gs://gokul_ibd_bucket/output.txt` via GCP Console or CLI:
    ```
    gsutil cp gs://gokul_ibd_bucket/output.txt .
    ```